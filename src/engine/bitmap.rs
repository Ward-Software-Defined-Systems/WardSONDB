use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use parking_lot::RwLock;
use roaring::RoaringBitmap;
use serde_json::Value;
use tracing::info;
use uuid::Uuid;

use crate::query::filter::{FilterNode, FilterOp, resolve_json_path};

/// Convert a JSON value to a deterministic string key for bitmap HashMap lookup.
pub fn value_to_string_key(value: &Value) -> String {
    match value {
        Value::Null => "__null__".to_string(),
        Value::Bool(true) => "__true__".to_string(),
        Value::Bool(false) => "__false__".to_string(),
        Value::Number(n) => format!("{}", n),
        Value::String(s) => s.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

// ── RowPositionMap ──────────────────────────────────────────────────────────

/// Bidirectional mapping between document IDs and row positions (u32).
pub struct RowPositionMap {
    id_to_pos: RwLock<HashMap<Arc<str>, u32>>,
    pos_to_id: RwLock<Vec<Option<Arc<str>>>>,
    next_pos: AtomicU32,
    hole_count: AtomicU32,
}

impl Default for RowPositionMap {
    fn default() -> Self {
        Self::new()
    }
}

impl RowPositionMap {
    pub fn new() -> Self {
        RowPositionMap {
            id_to_pos: RwLock::new(HashMap::new()),
            pos_to_id: RwLock::new(Vec::new()),
            next_pos: AtomicU32::new(0),
            hole_count: AtomicU32::new(0),
        }
    }

    /// Assign the next row position to a document ID.
    pub fn assign(&self, doc_id: &str) -> Option<u32> {
        let pos = self.next_pos.fetch_add(1, Ordering::Relaxed);
        let shared: Arc<str> = Arc::from(doc_id);
        self.id_to_pos.write().insert(Arc::clone(&shared), pos);
        let mut vec = self.pos_to_id.write();
        if pos as usize >= vec.len() {
            vec.resize(pos as usize + 1, None);
        }
        vec[pos as usize] = Some(shared);
        Some(pos)
    }

    /// Lookup row position by document ID.
    pub fn get_position(&self, doc_id: &str) -> Option<u32> {
        self.id_to_pos.read().get(doc_id).copied()
    }

    /// Lookup document ID by row position. Query paths batch through
    /// `resolve_window` instead; kept for single-position callers.
    #[allow(dead_code)]
    pub fn get_doc_id(&self, pos: u32) -> Option<Arc<str>> {
        let vec = self.pos_to_id.read();
        vec.get(pos as usize).and_then(|opt| opt.clone())
    }

    /// Remove a document from id_to_pos (position stays allocated; bitmap handles the hole).
    pub fn remove(&self, doc_id: &str) {
        let pos = self.id_to_pos.write().remove(doc_id);
        if let Some(pos) = pos {
            let mut vec = self.pos_to_id.write();
            if let Some(slot) = vec.get_mut(pos as usize) {
                *slot = None;
            }
            self.hole_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Number of active mappings.
    pub fn len(&self) -> u32 {
        self.id_to_pos.read().len() as u32
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.id_to_pos.read().is_empty()
    }

    pub fn clear(&self) {
        self.id_to_pos.write().clear();
        self.pos_to_id.write().clear();
        self.next_pos.store(0, Ordering::Relaxed);
        self.hole_count.store(0, Ordering::Relaxed);
    }

    /// Ratio of deleted (None) holes to total allocated positions.
    pub fn hole_ratio(&self) -> f32 {
        let total = self.next_pos.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let holes = self.hole_count.load(Ordering::Relaxed);
        holes as f32 / total as f32
    }

    /// Resolve bitmap positions (ascending order) to doc ids under ONE short
    /// guard, dropped before the caller does any IO — never hold a position
    /// or column guard across blocking work (the b965de5 rule). `skip`/`take`
    /// window the *resolved ids* (matching the materialized paths, which
    /// offset over docs), so hole positions from transient delete races are
    /// skipped without consuming the window.
    pub fn resolve_window(
        &self,
        positions: &RoaringBitmap,
        skip: usize,
        take: usize,
    ) -> Vec<Arc<str>> {
        let vec = self.pos_to_id.read();
        positions
            .iter()
            .filter_map(|pos| vec.get(pos as usize).and_then(|slot| slot.clone()))
            .skip(skip)
            .take(take)
            .collect()
    }

    /// Estimated memory usage in bytes, accounting for variable-length IDs.
    pub fn memory_bytes(&self) -> usize {
        let id_map = self.id_to_pos.read();
        let pos_vec = self.pos_to_id.read();
        // HashMap per-entry overhead: hash + bucket pointer + key (Arc ptr 8 bytes) + u32 value ≈ 48 bytes
        // Plus actual string bytes + Arc header (16 bytes) per unique ID
        let mut id_bytes: usize = id_map.len() * 48;
        for key in id_map.keys() {
            id_bytes += key.len() + 16; // string bytes + Arc header
        }
        // Vec: each slot is Option<Arc<str>> = 8 bytes (pointer-sized)
        let vec_bytes = pos_vec.len() * std::mem::size_of::<Option<Arc<str>>>();
        id_bytes + vec_bytes
    }
}

// ── BitmapColumn ────────────────────────────────────────────────────────────

/// One bitmap per distinct value for a tracked field.
pub struct BitmapColumn {
    #[allow(dead_code)]
    pub field_path: String,
    /// value_key -> RoaringBitmap of row positions
    pub value_bitmaps: RwLock<HashMap<String, RoaringBitmap>>,
    /// All positions that have this field (for $exists and $ne)
    pub exists_bitmap: RwLock<RoaringBitmap>,
    pub cardinality: AtomicU32,
}

impl BitmapColumn {
    pub fn new(field_path: String) -> Self {
        BitmapColumn {
            field_path,
            value_bitmaps: RwLock::new(HashMap::new()),
            exists_bitmap: RwLock::new(RoaringBitmap::new()),
            cardinality: AtomicU32::new(0),
        }
    }

    /// Estimated memory usage in bytes.
    pub fn memory_bytes(&self) -> usize {
        let bitmaps = self.value_bitmaps.read();
        let mut total = 0usize;
        for bitmap in bitmaps.values() {
            total += bitmap.serialized_size();
        }
        total += self.exists_bitmap.read().serialized_size();
        total
    }
}

/// Immutable per-column snapshot used by `persist_to_disk` so guards are
/// never held across blocking I/O. Fields mirror `BitmapColumn` but everything
/// is already cloned out from under the locks.
struct ColumnSnapshot {
    field_path: String,
    cardinality: u32,
    value_bitmaps: HashMap<String, RoaringBitmap>,
    exists_bitmap: RoaringBitmap,
}

// ── ScanAccelerator ─────────────────────────────────────────────────────────

/// Result of a bitmap scan: the matching positions + any residual filter.
pub struct BitmapScanResult {
    /// Bitmap of matching row positions.
    pub bitmap: RoaringBitmap,
    /// Filter conditions NOT covered by the bitmap (need post-filtering on loaded docs).
    pub residual_filter: Option<FilterNode>,
}

pub struct AcceleratorConfig {
    /// Fields to track with bitmaps.
    pub bitmap_fields: Vec<String>,
    /// Maximum distinct values per column before disabling that column.
    pub max_cardinality: u32,
    /// Maximum memory budget in bytes for all bitmap data. 0 = unlimited.
    pub max_memory_bytes: u64,
}

impl Default for AcceleratorConfig {
    fn default() -> Self {
        AcceleratorConfig {
            bitmap_fields: Vec::new(),
            max_cardinality: 1000,
            max_memory_bytes: 0,
        }
    }
}

pub struct ScanAccelerator {
    /// One BitmapColumn per tracked field.
    columns: RwLock<HashMap<String, BitmapColumn>>,
    /// Row position <-> document ID mapping.
    pub positions: RowPositionMap,
    /// Configuration.
    config: RwLock<AcceleratorConfig>,
    /// Cached copy of `config.max_cardinality` — read on every insert/update,
    /// so the hot path takes an atomic load instead of the config lock. Kept
    /// in sync by `set_max_cardinality`.
    max_cardinality: AtomicU32,
    /// false during rebuild; queries fall back to full scan.
    ready: AtomicBool,
    /// Cardinality profiler for auto-detection.
    profiler: CardinalityProfiler,
    /// true when memory budget is exceeded; skips bitmap column tracking.
    over_budget: AtomicBool,
    /// Cached total memory usage, refreshed by the background persist task.
    /// Read on the `on_insert` hot path to avoid the 4-lock `total_memory_bytes()` chain.
    cached_memory_bytes: AtomicU64,
}

impl ScanAccelerator {
    pub fn new(config: AcceleratorConfig) -> Self {
        let columns = RwLock::new(HashMap::new());
        // Pre-create columns for configured fields
        {
            let mut cols = columns.write();
            for field in &config.bitmap_fields {
                cols.insert(field.clone(), BitmapColumn::new(field.clone()));
            }
        }
        let has_fields = !config.bitmap_fields.is_empty();
        let max_cardinality = AtomicU32::new(config.max_cardinality);
        ScanAccelerator {
            columns,
            positions: RowPositionMap::new(),
            config: RwLock::new(config),
            max_cardinality,
            ready: AtomicBool::new(false),
            profiler: CardinalityProfiler::new(has_fields),
            over_budget: AtomicBool::new(false),
            cached_memory_bytes: AtomicU64::new(0),
        }
    }

    /// Set the per-column cardinality cap, keeping the hot-path atomic cache
    /// in sync with the config (guard-based mutation would bypass the cache).
    pub fn set_max_cardinality(&self, v: u32) {
        self.config.write().max_cardinality = v;
        self.max_cardinality.store(v, Ordering::Relaxed);
    }

    pub fn set_max_memory_bytes(&self, v: u64) {
        self.config.write().max_memory_bytes = v;
    }

    /// Number of inserts the cardinality profiler samples before it reports
    /// its `--bitmap-fields` recommendation (`--bitmap-sample-size`).
    pub fn set_sample_size(&self, n: u32) {
        self.profiler.set_sample_target(n);
    }

    pub fn config_read(&self) -> parking_lot::RwLockReadGuard<'_, AcceleratorConfig> {
        self.config.read()
    }

    /// Total estimated memory usage across all bitmap data.
    pub fn total_memory_bytes(&self) -> usize {
        let columns = self.columns.read();
        let mut total: usize = self.positions.memory_bytes();
        for column in columns.values() {
            total += column.memory_bytes();
        }
        total
    }

    /// Refresh `cached_memory_bytes`. Called from the background persist task
    /// (inside `spawn_blocking`) so the `on_insert` hot path can read a cached
    /// value via a single atomic load instead of acquiring four RwLock guards.
    pub fn recompute_cached_memory(&self) {
        let bytes = self.total_memory_bytes() as u64;
        self.cached_memory_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }

    pub fn is_over_budget(&self) -> bool {
        self.over_budget.load(Ordering::Relaxed)
    }

    /// Configure bitmap fields and create columns.
    pub fn configure_fields(&self, fields: Vec<String>) {
        let mut cols = self.columns.write();
        for field in &fields {
            if !cols.contains_key(field) {
                cols.insert(field.clone(), BitmapColumn::new(field.clone()));
            }
        }
        self.config.write().bitmap_fields = fields;
    }

    /// Check if the accelerator has any bitmap columns configured.
    pub fn has_columns(&self) -> bool {
        !self.columns.read().is_empty()
    }

    /// Check if position map has excessive holes from TTL deletes (>25%).
    pub fn needs_compaction(&self) -> bool {
        self.positions.hole_ratio() > 0.25
    }

    // ── CRUD Hooks ──────────────────────────────────────────────────────

    /// Called after a document insert transaction commits.
    pub fn on_insert(&self, doc_id: &str, doc: &Value) {
        let pos = match self.positions.assign(doc_id) {
            Some(p) => p,
            None => return,
        };

        // Feed the profiler during sampling phase
        if !self.profiler.is_done() {
            self.profiler.observe(doc);
            // Check if profiling just completed (we hit sample_target)
            if self.profiler.is_done() && self.columns.read().is_empty() {
                let max_card = self.max_cardinality.load(Ordering::Relaxed);
                let detected = self.profiler.analyze(max_card);
                if !detected.is_empty() {
                    let field_info: Vec<String> = detected
                        .iter()
                        .map(|(f, c)| format!("{f} ({c} values)"))
                        .collect();
                    let flag_value: Vec<String> = detected.iter().map(|(f, _)| f.clone()).collect();
                    // Recommendation ONLY — never create columns here. Docs
                    // inserted before detection were only profiled, so columns
                    // born now would be missing them forever (no safe live
                    // rebuild exists yet), and create_collection's re-arm
                    // (set_ready on has_columns) would start serving those
                    // incomplete bitmaps: silent false negatives. Activation
                    // requires --bitmap-fields at startup, which rebuilds from
                    // storage before serving.
                    info!(
                        fields = %field_info.join(", "),
                        flag = %format!("--bitmap-fields {}", flag_value.join(",")),
                        "Scan accelerator: low-cardinality fields detected — \
                         inactive; restart with the suggested flag to enable"
                    );
                }
                self.profiler.finish();
            }
        }

        // Check memory budget every 1000 inserts.
        // Reads `cached_memory_bytes` (a single atomic) instead of calling
        // `total_memory_bytes()` — the latter takes four RwLock reader guards
        // (config + columns + positions.id_to_pos + positions.pos_to_id) and
        // contributed to a write-halt deadlock against `persist_to_disk`.
        // The cache is refreshed by the background persist task every 60 s.
        if pos % 1000 == 0 {
            let budget = self.config.read().max_memory_bytes;
            if budget > 0 {
                let used = self.cached_memory_bytes.load(Ordering::Relaxed) as usize;
                let was_over = self.over_budget.load(Ordering::Relaxed);
                let is_over = used as u64 > budget;
                if is_over != was_over {
                    self.over_budget.store(is_over, Ordering::Relaxed);
                    if is_over {
                        tracing::warn!(
                            used_mb = used / (1024 * 1024),
                            budget_mb = budget / (1024 * 1024),
                            "Bitmap memory budget exceeded, pausing column tracking"
                        );
                    }
                }
            }
        }

        if self.over_budget.load(Ordering::Relaxed) {
            return;
        }

        let columns = self.columns.read();
        let max_card = self.max_cardinality.load(Ordering::Relaxed);

        for (field_path, column) in columns.iter() {
            if let Some(value) = resolve_json_path(doc, field_path) {
                let value_key = value_to_string_key(value);

                if column.cardinality.load(Ordering::Relaxed) < max_card
                    || column.value_bitmaps.read().contains_key(&value_key)
                {
                    let mut bitmaps = column.value_bitmaps.write();
                    let is_new = !bitmaps.contains_key(&value_key);
                    bitmaps
                        .entry(value_key)
                        .or_insert_with(RoaringBitmap::new)
                        .insert(pos);
                    if is_new {
                        column
                            .cardinality
                            .store(bitmaps.len() as u32, Ordering::Relaxed);
                    }
                }

                column.exists_bitmap.write().insert(pos);
            }
        }
    }

    /// Called after a document delete transaction commits.
    pub fn on_delete(&self, doc_id: &str, doc: &Value) {
        let pos = match self.positions.get_position(doc_id) {
            Some(p) => p,
            None => return,
        };

        let columns = self.columns.read();
        for (field_path, column) in columns.iter() {
            if let Some(value) = resolve_json_path(doc, field_path) {
                let value_key = value_to_string_key(value);
                let mut bitmaps = column.value_bitmaps.write();
                let mut remove_key = false;
                if let Some(bitmap) = bitmaps.get_mut(&value_key) {
                    bitmap.remove(pos);
                    if bitmap.is_empty() {
                        remove_key = true;
                    }
                }
                if remove_key {
                    bitmaps.remove(&value_key);
                    column.cardinality.fetch_sub(1, Ordering::Relaxed);
                }
                drop(bitmaps);
                // Inside the field-present guard (symmetric with on_insert):
                // exists_bitmap only ever holds positions whose doc had the
                // field, so absent fields need no write lock here.
                column.exists_bitmap.write().remove(pos);
            }
        }
        self.positions.remove(doc_id);
    }

    /// Called after a document update transaction commits.
    /// Uses a single write lock acquisition per column.
    pub fn on_update(&self, doc_id: &str, old_doc: &Value, new_doc: &Value) {
        let pos = match self.positions.get_position(doc_id) {
            Some(p) => p,
            None => return,
        };

        let columns = self.columns.read();
        let max_card = self.max_cardinality.load(Ordering::Relaxed);

        for (field_path, column) in columns.iter() {
            // Compare the resolved values first: an update that doesn't touch
            // this column costs zero allocations. Only genuine changes pay
            // for the string keys (whose comparison stays authoritative —
            // distinct Values can share a key, e.g. null vs "__null__", and
            // must keep no-oping).
            let old_ref = resolve_json_path(old_doc, field_path);
            let new_ref = resolve_json_path(new_doc, field_path);
            if old_ref == new_ref {
                continue;
            }
            let old_val = old_ref.map(value_to_string_key);
            let new_val = new_ref.map(value_to_string_key);

            if old_val != new_val {
                // Single write lock for both remove and insert
                let mut bitmaps = column.value_bitmaps.write();

                // Remove from old bitmap
                if let Some(old_key) = &old_val {
                    let mut remove_key = false;
                    if let Some(bitmap) = bitmaps.get_mut(old_key) {
                        bitmap.remove(pos);
                        if bitmap.is_empty() {
                            remove_key = true;
                        }
                    }
                    if remove_key {
                        bitmaps.remove(old_key);
                        column.cardinality.fetch_sub(1, Ordering::Relaxed);
                    }
                }

                // Add to new bitmap
                if let Some(new_key) = &new_val {
                    let card = column.cardinality.load(Ordering::Relaxed);
                    if card < max_card || bitmaps.contains_key(new_key) {
                        let is_new = !bitmaps.contains_key(new_key);
                        bitmaps
                            .entry(new_key.clone())
                            .or_insert_with(RoaringBitmap::new)
                            .insert(pos);
                        if is_new {
                            column
                                .cardinality
                                .store(bitmaps.len() as u32, Ordering::Relaxed);
                        }
                    }
                }

                drop(bitmaps); // Release write lock before exists_bitmap lock

                // Update exists bitmap
                let mut exists = column.exists_bitmap.write();
                if old_val.is_some() && new_val.is_none() {
                    exists.remove(pos);
                } else if old_val.is_none() && new_val.is_some() {
                    exists.insert(pos);
                }
            }
        }
    }

    /// Clear all accelerator data for a collection (called on drop_collection).
    pub fn clear(&self) {
        self.positions.clear();
        let mut cols = self.columns.write();
        for column in cols.values_mut() {
            column.value_bitmaps.write().clear();
            *column.exists_bitmap.write() = RoaringBitmap::new();
            column.cardinality.store(0, Ordering::Relaxed);
        }
        self.ready.store(false, Ordering::Release);
        self.over_budget.store(false, Ordering::Relaxed);
        self.profiler.reset();
    }

    /// Rebuild the accelerator from all documents in storage (used by benchmarks).
    #[allow(dead_code)]
    pub fn rebuild_from_storage(&self, docs: &[(String, Value)]) {
        self.ready.store(false, Ordering::Release);

        let start = std::time::Instant::now();
        for (doc_id, doc) in docs {
            self.on_insert(doc_id, doc);
        }

        let elapsed = start.elapsed();
        let count = docs.len();
        let cols = self.columns.read();
        let col_names: Vec<&str> = cols.keys().map(|s| s.as_str()).collect();
        info!(
            docs = count,
            elapsed_ms = elapsed.as_millis(),
            fields = ?col_names,
            "Scan accelerator rebuilt"
        );

        self.ready.store(true, Ordering::Release);
    }

    /// Process a batch of documents during incremental rebuild.
    /// Stops early if the memory budget is exceeded.
    pub fn rebuild_batch(&self, docs: &[(String, Value)]) {
        for (doc_id, doc) in docs {
            if self.over_budget.load(Ordering::Relaxed) {
                return;
            }
            self.on_insert(doc_id, doc);
        }
    }

    // ── Query (bitmap_scan) ─────────────────────────────────────────────

    /// Attempt to resolve a filter entirely or partially via bitmaps.
    /// Returns None if the filter cannot be handled by bitmaps at all.
    pub fn bitmap_scan(&self, filter: &FilterNode) -> Option<BitmapScanResult> {
        if !self.is_ready() {
            return None;
        }
        self.bitmap_scan_inner(filter)
    }

    fn bitmap_scan_inner(&self, filter: &FilterNode) -> Option<BitmapScanResult> {
        match filter {
            FilterNode::Comparison {
                field,
                op: FilterOp::Eq,
                value,
            } => {
                let columns = self.columns.read();
                let column = columns.get(field)?;
                let key = value_to_string_key(value);
                let bitmaps = column.value_bitmaps.read();
                let bitmap = bitmaps.get(&key).cloned().unwrap_or_default();
                Some(BitmapScanResult {
                    bitmap,
                    residual_filter: None,
                })
            }

            FilterNode::Comparison {
                field,
                op: FilterOp::Ne,
                value,
            } => {
                let columns = self.columns.read();
                let column = columns.get(field)?;
                let key = value_to_string_key(value);
                let bitmaps = column.value_bitmaps.read();
                let exists = column.exists_bitmap.read().clone();
                let bitmap = match bitmaps.get(&key) {
                    Some(eq_bitmap) => &exists - eq_bitmap,
                    None => exists,
                };
                Some(BitmapScanResult {
                    bitmap,
                    residual_filter: None,
                })
            }

            FilterNode::Comparison {
                field,
                op: FilterOp::In,
                value,
            } => {
                let columns = self.columns.read();
                let column = columns.get(field)?;
                let values = value.as_array()?;
                let bitmaps = column.value_bitmaps.read();
                let mut result = RoaringBitmap::new();
                for v in values {
                    let key = value_to_string_key(v);
                    if let Some(bitmap) = bitmaps.get(&key) {
                        result |= bitmap;
                    }
                }
                Some(BitmapScanResult {
                    bitmap: result,
                    residual_filter: None,
                })
            }

            FilterNode::Comparison {
                field,
                op: FilterOp::Exists,
                value,
            } => {
                let should_exist = value.as_bool().unwrap_or(true);
                let columns = self.columns.read();
                let column = columns.get(field)?;
                let exists = column.exists_bitmap.read().clone();
                if should_exist {
                    Some(BitmapScanResult {
                        bitmap: exists,
                        residual_filter: None,
                    })
                } else {
                    // $exists: false — need all positions minus exists bitmap.
                    // We don't have a "universe" bitmap, so fall back.
                    None
                }
            }

            FilterNode::And(children) => {
                let mut result: Option<RoaringBitmap> = None;
                let mut residual_children: Vec<FilterNode> = Vec::new();

                for child in children {
                    match self.bitmap_scan_inner(child) {
                        Some(BitmapScanResult {
                            bitmap,
                            residual_filter,
                        }) => {
                            result = Some(match result {
                                Some(existing) => existing & &bitmap,
                                None => bitmap,
                            });
                            if let Some(residual) = residual_filter {
                                residual_children.push(residual);
                            }
                        }
                        None => {
                            residual_children.push(child.clone());
                        }
                    }
                }

                let bitmap = result?; // At least one child must be bitmap-resolvable

                let residual = match residual_children.len() {
                    0 => None,
                    1 => Some(residual_children.into_iter().next().unwrap()),
                    _ => Some(FilterNode::And(residual_children)),
                };

                Some(BitmapScanResult {
                    bitmap,
                    residual_filter: residual,
                })
            }

            FilterNode::Or(children) => {
                // A $or is bitmap-servable ONLY with full coverage: the plan's
                // residual_filter is applied as a CONJUNCTION over bitmap-matched
                // docs, so a partially-covered $or would execute as an intersection
                // (covered-arm docs post-filtered by the uncovered arm; uncovered-arm
                // docs never loaded). Partial coverage bails to the fallback
                // strategies; per-arm union planning is H-P3.1 territory.
                if children.is_empty() {
                    return None;
                }
                let mut bitmap_result = RoaringBitmap::new();
                for child in children {
                    match self.bitmap_scan_inner(child) {
                        Some(BitmapScanResult {
                            bitmap,
                            residual_filter: None,
                        }) => {
                            bitmap_result |= &bitmap;
                        }
                        _ => return None,
                    }
                }
                Some(BitmapScanResult {
                    bitmap: bitmap_result,
                    residual_filter: None,
                })
            }

            // $not and other ops — not bitmap-eligible
            _ => None,
        }
    }

    // ── Aggregation helpers ─────────────────────────────────────────────

    /// Count documents per value for a bitmap field (for $group + $count aggregation).
    /// Returns None if the field doesn't have a bitmap column.
    pub fn count_by_field(&self, field: &str) -> Option<Vec<(String, u64)>> {
        let columns = self.columns.read();
        let column = columns.get(field)?;
        let bitmaps = column.value_bitmaps.read();
        let result: Vec<(String, u64)> = bitmaps
            .iter()
            .map(|(value, bitmap)| (value.clone(), bitmap.len()))
            .collect();
        Some(result)
    }

    /// Count documents per value for a bitmap field, filtered by a match bitmap.
    /// For $match + $group + $count aggregation.
    pub fn count_by_field_filtered(
        &self,
        field: &str,
        match_bitmap: &RoaringBitmap,
    ) -> Option<Vec<(String, u64)>> {
        let columns = self.columns.read();
        let column = columns.get(field)?;
        let bitmaps = column.value_bitmaps.read();
        let mut result = Vec::new();
        for (value, bitmap) in bitmaps.iter() {
            let count = (bitmap & match_bitmap).len();
            if count > 0 {
                result.push((value.clone(), count));
            }
        }
        Some(result)
    }

    /// Get a column reference for a field (checks if it exists).
    pub fn has_column(&self, field: &str) -> bool {
        self.columns.read().contains_key(field)
    }

    // ── Stats ───────────────────────────────────────────────────────────

    pub fn stats(&self) -> AcceleratorStats {
        // Single `columns` guard scope: calling total_memory_bytes() while
        // holding it re-acquires `columns.read()`, and parking_lot readers
        // queued behind a waiting writer (configure_fields / auto-detect /
        // clear / load_from_disk) deadlock on re-entry. Sum column memory in
        // the same pass instead and touch every other lock after the guard
        // drops.
        let mut column_stats = Vec::new();
        let mut columns_memory = 0usize;
        {
            let columns = self.columns.read();
            for (field, column) in columns.iter() {
                let memory_bytes = column.memory_bytes();
                columns_memory += memory_bytes;
                column_stats.push(ColumnStat {
                    field: field.clone(),
                    cardinality: column.cardinality.load(Ordering::Relaxed),
                    memory_bytes,
                });
            }
        }
        AcceleratorStats {
            ready: self.is_ready(),
            total_positions: self.positions.len(),
            columns: column_stats,
            memory_bytes: columns_memory + self.positions.memory_bytes(),
            memory_budget_bytes: self.config.read().max_memory_bytes,
            over_budget: self.over_budget.load(Ordering::Relaxed),
        }
    }

    #[allow(dead_code)]
    pub fn profiler(&self) -> &CardinalityProfiler {
        &self.profiler
    }

    // ── Disk Persistence ────────────────────────────────────────────────

    /// Persist all bitmaps and position map to disk.
    pub fn persist_to_disk(&self, data_dir: &Path, collection: &str) -> Result<(), std::io::Error> {
        if !self.is_ready() || !self.has_columns() {
            return Ok(());
        }

        let bitmap_dir = data_dir.join("bitmap").join(collection);
        fs::create_dir_all(&bitmap_dir)?;

        // Snapshot everything we need under the shortest possible guard scopes,
        // then drop all guards before any blocking I/O. Earlier revisions held
        // reader guards on `pos_to_id` + `id_to_pos` + `columns` + per-column
        // `value_bitmaps` + `exists_bitmap` across `fs::write` calls. Under
        // parking_lot writer-fairness, a queued writer from `on_insert` /
        // `on_update` would cause every subsequent reader in this function to
        // block, producing a hard process-wide deadlock under sustained ingest.
        //
        // Peak transient allocation at 2M docs × ~11 fields: ~300-400 MB
        // (dominated by the positions Vec + HashMap clones). Budget for this
        // if pushing past 10M docs on small hosts.
        let (pos_vec_snapshot, next_pos) = {
            let guard = self.positions.pos_to_id.read();
            (
                guard.clone(),
                self.positions.next_pos.load(Ordering::Relaxed),
            )
        };

        let id_map_snapshot: HashMap<Arc<str>, u32> = self.positions.id_to_pos.read().clone();

        let columns_snapshot: Vec<ColumnSnapshot> = {
            let cols = self.columns.read();
            cols.iter()
                .map(|(field, col)| ColumnSnapshot {
                    field_path: field.clone(),
                    cardinality: col.cardinality.load(Ordering::Relaxed),
                    value_bitmaps: col.value_bitmaps.read().clone(),
                    exists_bitmap: col.exists_bitmap.read().clone(),
                })
                .collect()
        };

        // From here on, no RwLock guards are held. All I/O runs lock-free.

        let meta = serde_json::json!({
            "next_pos": next_pos,
            "count": pos_vec_snapshot.len(),
        });
        fs::write(
            bitmap_dir.join("positions.meta.json"),
            serde_json::to_string_pretty(&meta).unwrap_or_default(),
        )?;

        // Serde's `Arc<T>` Serialize impl is gated behind the `rc` feature,
        // which we don't enable. Convert to borrowed `&str` references at
        // serialize time — cheap, no allocations, and produces the same
        // JSON bytes as the previous implementation.
        let pos_entries: Vec<Option<&str>> = pos_vec_snapshot
            .iter()
            .map(|slot| slot.as_deref())
            .collect();
        fs::write(
            bitmap_dir.join("positions.map.json"),
            serde_json::to_string(&pos_entries).unwrap_or_default(),
        )?;

        let id_entries: HashMap<&str, u32> =
            id_map_snapshot.iter().map(|(k, v)| (&**k, *v)).collect();
        fs::write(
            bitmap_dir.join("positions.ids.json"),
            serde_json::to_string(&id_entries).unwrap_or_default(),
        )?;

        let mut columns_meta = Vec::new();
        for snapshot in &columns_snapshot {
            let safe_field = snapshot.field_path.replace('.', "_DOT_");

            // Write exists bitmap
            let mut exists_bytes = Vec::new();
            snapshot
                .exists_bitmap
                .serialize_into(&mut exists_bytes)
                .map_err(std::io::Error::other)?;
            fs::write(
                bitmap_dir.join(format!("{safe_field}.exists.roaring")),
                &exists_bytes,
            )?;

            // Write each value bitmap
            let mut value_keys = Vec::new();
            for (i, (value_key, bitmap)) in snapshot.value_bitmaps.iter().enumerate() {
                let mut bitmap_bytes = Vec::new();
                bitmap
                    .serialize_into(&mut bitmap_bytes)
                    .map_err(std::io::Error::other)?;
                let filename = format!("{safe_field}_v{i}.roaring");
                fs::write(bitmap_dir.join(&filename), &bitmap_bytes)?;
                value_keys.push((value_key.clone(), filename));
            }

            columns_meta.push(serde_json::json!({
                "field_path": &snapshot.field_path,
                "safe_field": safe_field,
                "cardinality": snapshot.cardinality,
                "values": value_keys.iter().map(|(k, f)| serde_json::json!({"key": k, "file": f})).collect::<Vec<_>>(),
            }));
        }

        let columns_meta_json = serde_json::json!({ "columns": columns_meta });
        fs::write(
            bitmap_dir.join("columns.meta.json"),
            serde_json::to_string_pretty(&columns_meta_json).unwrap_or_default(),
        )?;

        Ok(())
    }

    /// Try to load bitmaps from disk. Returns true on success.
    pub fn load_from_disk(&self, data_dir: &Path, collection: &str) -> bool {
        let bitmap_dir = data_dir.join("bitmap").join(collection);
        if !bitmap_dir.exists() {
            return false;
        }

        // Load position metadata
        let meta_path = bitmap_dir.join("positions.meta.json");
        let meta_str = match fs::read_to_string(&meta_path) {
            Ok(s) => s,
            Err(_) => return false,
        };
        let meta: Value = match serde_json::from_str(&meta_str) {
            Ok(v) => v,
            Err(_) => return false,
        };
        let next_pos = meta.get("next_pos").and_then(|v| v.as_u64()).unwrap_or(0) as u32;

        // Load position map (JSON format — supports variable-length string IDs)
        // Try new JSON format first, fall back to legacy binary format
        let (pos_vec, id_map) = if let Ok(json_data) =
            fs::read_to_string(bitmap_dir.join("positions.map.json"))
        {
            let pos_vec: Vec<Option<String>> = match serde_json::from_str(&json_data) {
                Ok(v) => v,
                Err(_) => return false,
            };
            let id_json = match fs::read_to_string(bitmap_dir.join("positions.ids.json")) {
                Ok(d) => d,
                Err(_) => return false,
            };
            let id_map: HashMap<String, u32> = match serde_json::from_str(&id_json) {
                Ok(v) => v,
                Err(_) => return false,
            };
            (pos_vec, id_map)
        } else {
            // Legacy binary format (UUID-only, 16 bytes per entry)
            let pos_data = match fs::read(bitmap_dir.join("positions.map.bin")) {
                Ok(d) => d,
                Err(_) => return false,
            };
            let mut pos_vec = Vec::new();
            let mut i = 0;
            while i + 17 <= pos_data.len() {
                let present = pos_data[i];
                i += 1;
                if present == 1 {
                    let uuid = Uuid::from_bytes(pos_data[i..i + 16].try_into().unwrap_or([0; 16]));
                    pos_vec.push(Some(uuid.to_string()));
                } else {
                    pos_vec.push(None);
                }
                i += 16;
            }
            let id_data = match fs::read(bitmap_dir.join("positions.ids.bin")) {
                Ok(d) => d,
                Err(_) => return false,
            };
            let mut id_map = HashMap::new();
            let mut j = 0;
            while j + 20 <= id_data.len() {
                let uuid = Uuid::from_bytes(id_data[j..j + 16].try_into().unwrap_or([0; 16]));
                let pos = u32::from_le_bytes([
                    id_data[j + 16],
                    id_data[j + 17],
                    id_data[j + 18],
                    id_data[j + 19],
                ]);
                id_map.insert(uuid.to_string(), pos);
                j += 20;
            }
            (pos_vec, id_map)
        };

        // Install position data (convert String → Arc<str>)
        *self.positions.id_to_pos.write() = id_map
            .into_iter()
            .map(|(k, v)| (Arc::from(k.as_str()), v))
            .collect();
        *self.positions.pos_to_id.write() = pos_vec
            .into_iter()
            .map(|opt| opt.map(|s| Arc::from(s.as_str())))
            .collect();
        self.positions.next_pos.store(next_pos, Ordering::Relaxed);

        // Load columns metadata
        let cols_meta_str = match fs::read_to_string(bitmap_dir.join("columns.meta.json")) {
            Ok(s) => s,
            Err(_) => return false,
        };
        let cols_meta: Value = match serde_json::from_str(&cols_meta_str) {
            Ok(v) => v,
            Err(_) => return false,
        };

        let cols_arr = match cols_meta.get("columns").and_then(|v| v.as_array()) {
            Some(a) => a,
            None => return false,
        };

        let mut columns = self.columns.write();

        for col_meta in cols_arr {
            let field_path = match col_meta.get("field_path").and_then(|v| v.as_str()) {
                Some(s) => s.to_string(),
                None => continue,
            };
            let safe_field = match col_meta.get("safe_field").and_then(|v| v.as_str()) {
                Some(s) => s.to_string(),
                None => continue,
            };
            let cardinality = col_meta
                .get("cardinality")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as u32;

            // Load exists bitmap
            let exists_bytes =
                match fs::read(bitmap_dir.join(format!("{safe_field}.exists.roaring"))) {
                    Ok(d) => d,
                    Err(_) => continue,
                };
            let exists_bitmap = match RoaringBitmap::deserialize_from(&exists_bytes[..]) {
                Ok(b) => b,
                Err(_) => continue,
            };

            // Load value bitmaps
            let values = match col_meta.get("values").and_then(|v| v.as_array()) {
                Some(a) => a,
                None => continue,
            };

            let mut value_bitmaps = HashMap::new();
            for val_entry in values {
                let key = match val_entry.get("key").and_then(|v| v.as_str()) {
                    Some(s) => s.to_string(),
                    None => continue,
                };
                let file = match val_entry.get("file").and_then(|v| v.as_str()) {
                    Some(s) => s,
                    None => continue,
                };
                let bitmap_bytes = match fs::read(bitmap_dir.join(file)) {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                let bitmap = match RoaringBitmap::deserialize_from(&bitmap_bytes[..]) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                value_bitmaps.insert(key, bitmap);
            }

            let column = columns
                .entry(field_path.clone())
                .or_insert_with(|| BitmapColumn::new(field_path));
            *column.value_bitmaps.write() = value_bitmaps;
            *column.exists_bitmap.write() = exists_bitmap;
            column.cardinality.store(cardinality, Ordering::Relaxed);
        }

        info!(
            collection = collection,
            positions = next_pos,
            columns = cols_arr.len(),
            "Scan accelerator loaded from disk"
        );

        true
    }
}

// ── AcceleratorStats ────────────────────────────────────────────────────────

pub struct AcceleratorStats {
    pub ready: bool,
    pub total_positions: u32,
    pub columns: Vec<ColumnStat>,
    pub memory_bytes: usize,
    pub memory_budget_bytes: u64,
    pub over_budget: bool,
}

pub struct ColumnStat {
    pub field: String,
    pub cardinality: u32,
    pub memory_bytes: usize,
}

// ── CardinalityProfiler ─────────────────────────────────────────────────────

/// Profiles field cardinality during the first N inserts to auto-detect
/// which fields are suitable for bitmap tracking.
pub struct CardinalityProfiler {
    /// field_path -> set of observed distinct values
    observed: RwLock<HashMap<String, HashSet<String>>>,
    sample_count: AtomicU32,
    sample_target: AtomicU32,
    done: AtomicBool,
    /// If true, skip profiling (fields were explicitly configured)
    skip: bool,
}

impl CardinalityProfiler {
    pub fn new(skip: bool) -> Self {
        CardinalityProfiler {
            observed: RwLock::new(HashMap::new()),
            sample_count: AtomicU32::new(0),
            sample_target: AtomicU32::new(10_000),
            done: AtomicBool::new(skip),
            skip,
        }
    }

    /// Set how many inserts to profile before detection completes
    /// (`--bitmap-sample-size`). Was a no-op placeholder until the flag was
    /// wired up; a zero target is clamped to 1 so detection still terminates.
    pub fn set_sample_target(&self, target: u32) {
        self.sample_target.store(target.max(1), Ordering::Relaxed);
    }

    pub fn is_done(&self) -> bool {
        self.done.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        if !self.skip {
            self.observed.write().clear();
            self.sample_count.store(0, Ordering::Relaxed);
            self.done.store(false, Ordering::Relaxed);
        }
    }

    /// Observe a document during the profiling phase.
    fn observe(&self, doc: &Value) {
        if self.done.load(Ordering::Relaxed) {
            return;
        }

        let count = self.sample_count.fetch_add(1, Ordering::Relaxed) + 1;

        if let Some(obj) = doc.as_object() {
            let mut observed = self.observed.write();
            for (key, value) in obj {
                // Skip system fields
                if key.starts_with('_') {
                    continue;
                }

                // Top-level field
                let val_key = value_to_string_key(value);
                observed.entry(key.clone()).or_default().insert(val_key);

                // One level of nesting
                if let Value::Object(inner) = value {
                    for (inner_key, inner_value) in inner {
                        if inner_key.starts_with('_') {
                            continue;
                        }
                        let path = format!("{key}.{inner_key}");
                        let val_key = value_to_string_key(inner_value);
                        observed.entry(path).or_default().insert(val_key);
                    }
                }
            }
        }

        if count >= self.sample_target.load(Ordering::Relaxed) {
            self.done.store(true, Ordering::Relaxed);
        }
    }

    /// Analyze profiled data and return fields suitable for bitmap tracking.
    pub fn analyze(&self, max_cardinality: u32) -> Vec<(String, u32)> {
        let observed = self.observed.read();
        let mut results: Vec<(String, u32)> = Vec::new();

        for (field, values) in observed.iter() {
            let card = values.len() as u32;
            if card > 0 && card < max_cardinality {
                results.push((field.clone(), card));
            } else if card >= max_cardinality {
                info!(
                    field = field,
                    cardinality = card,
                    "Skipping bitmap for high-cardinality field"
                );
            }
        }

        // Sort by cardinality ascending (most selective first)
        results.sort_by_key(|(_, c)| *c);
        results
    }

    /// Consume the profiler's observed data (frees memory).
    pub fn finish(&self) {
        self.observed.write().clear();
        self.done.store(true, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn sample_count(&self) -> u32 {
        self.sample_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn accel(fields: &[&str]) -> ScanAccelerator {
        let a = ScanAccelerator::new(AcceleratorConfig {
            bitmap_fields: fields.iter().map(|s| s.to_string()).collect(),
            max_cardinality: 1000,
            max_memory_bytes: 0,
        });
        a.set_ready(true);
        a
    }

    fn exists_len(a: &ScanAccelerator, field: &str) -> u64 {
        a.columns
            .read()
            .get(field)
            .unwrap()
            .exists_bitmap
            .read()
            .len()
    }

    /// The invariant that lets on_delete keep the exists write lock inside
    /// the field-present guard: exists_bitmap never contains positions of
    /// docs lacking the field, so absent fields have nothing to remove.
    #[test]
    fn on_delete_missing_field_leaves_other_columns_alone() {
        let a = accel(&["kind", "tier"]);
        a.on_insert("doc-1", &json!({"kind": "a"})); // no tier
        a.on_insert("doc-2", &json!({"kind": "b", "tier": "x"}));
        assert_eq!(exists_len(&a, "kind"), 2);
        assert_eq!(exists_len(&a, "tier"), 1);

        a.on_delete("doc-1", &json!({"kind": "a"}));
        assert_eq!(exists_len(&a, "kind"), 1);
        assert_eq!(exists_len(&a, "tier"), 1); // untouched — doc-1 had none
    }

    /// An update that doesn't change a column's value must leave its bitmaps
    /// byte-identical (the zero-allocation fast path is a pure no-op).
    #[test]
    fn on_update_unchanged_column_is_noop() {
        let a = accel(&["kind", "tier"]);
        a.on_insert("doc-1", &json!({"kind": "a", "tier": "x"}));
        a.on_insert("doc-2", &json!({"kind": "a", "tier": "y"}));

        // Same values on both sides for kind; only tier changes.
        a.on_update(
            "doc-1",
            &json!({"kind": "a", "tier": "x"}),
            &json!({"kind": "a", "tier": "z"}),
        );

        let cols = a.columns.read();
        let kind = cols.get("kind").unwrap();
        assert_eq!(kind.cardinality.load(Ordering::Relaxed), 1);
        assert_eq!(kind.value_bitmaps.read().get("a").unwrap().len(), 2);
        let tier = cols.get("tier").unwrap();
        let tiers = tier.value_bitmaps.read();
        assert!(!tiers.contains_key("x"));
        assert_eq!(tiers.get("z").unwrap().len(), 1);
        assert_eq!(tiers.get("y").unwrap().len(), 1);
    }

    /// set_max_cardinality must be visible to subsequent inserts (the hot
    /// path reads the atomic cache, not the config lock).
    #[test]
    fn set_max_cardinality_applies_to_next_insert() {
        let a = accel(&["kind"]);
        a.set_max_cardinality(2);
        a.on_insert("d1", &json!({"kind": "a"}));
        a.on_insert("d2", &json!({"kind": "b"}));
        a.on_insert("d3", &json!({"kind": "c"})); // over the cap — not tracked

        let cols = a.columns.read();
        let kind = cols.get("kind").unwrap();
        assert_eq!(kind.cardinality.load(Ordering::Relaxed), 2);
        assert!(!kind.value_bitmaps.read().contains_key("c"));
        // Presence is still tracked past the cap.
        assert_eq!(kind.exists_bitmap.read().len(), 3);
    }

    /// resolve_window windows over resolved ids (holes from deletes are
    /// skipped without consuming the window) in ascending position order.
    #[test]
    fn resolve_window_skips_holes_without_consuming_window() {
        let a = accel(&["kind"]);
        for i in 0..5 {
            a.on_insert(&format!("doc-{i}"), &json!({"kind": "a"}));
        }
        a.on_delete("doc-2", &json!({"kind": "a"})); // hole at position 2

        let all: RoaringBitmap = (0u32..5).collect();
        let ids = a.positions.resolve_window(&all, 0, usize::MAX);
        assert_eq!(
            ids.iter().map(|s| s.as_ref()).collect::<Vec<_>>(),
            ["doc-0", "doc-1", "doc-3", "doc-4"]
        );

        let ids = a.positions.resolve_window(&all, 1, 2);
        assert_eq!(
            ids.iter().map(|s| s.as_ref()).collect::<Vec<_>>(),
            ["doc-1", "doc-3"]
        );
    }
}
