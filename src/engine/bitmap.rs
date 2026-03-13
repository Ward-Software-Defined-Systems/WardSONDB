use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use parking_lot::RwLock;
use roaring::RoaringBitmap;
use serde_json::Value;
use tracing::info;
use uuid::Uuid;

use crate::query::filter::{FilterNode, FilterOp, resolve_json_path};

/// Compact UUID storage — 16 bytes instead of 36-byte hex string.
type DocIdBytes = [u8; 16];

fn doc_id_to_bytes(id: &str) -> Option<DocIdBytes> {
    Uuid::parse_str(id).ok().map(|u| *u.as_bytes())
}

fn bytes_to_doc_id(bytes: &DocIdBytes) -> String {
    Uuid::from_bytes(*bytes).to_string()
}

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
    id_to_pos: RwLock<HashMap<DocIdBytes, u32>>,
    pos_to_id: RwLock<Vec<Option<DocIdBytes>>>,
    next_pos: AtomicU32,
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
        }
    }

    /// Assign the next row position to a document ID.
    pub fn assign(&self, doc_id: &str) -> Option<u32> {
        let bytes = doc_id_to_bytes(doc_id)?;
        let pos = self.next_pos.fetch_add(1, Ordering::Relaxed);
        self.id_to_pos.write().insert(bytes, pos);
        let mut vec = self.pos_to_id.write();
        if pos as usize >= vec.len() {
            vec.resize(pos as usize + 1, None);
        }
        vec[pos as usize] = Some(bytes);
        Some(pos)
    }

    /// Lookup row position by document ID.
    pub fn get_position(&self, doc_id: &str) -> Option<u32> {
        let bytes = doc_id_to_bytes(doc_id)?;
        self.id_to_pos.read().get(&bytes).copied()
    }

    /// Lookup document ID by row position.
    pub fn get_doc_id(&self, pos: u32) -> Option<String> {
        let vec = self.pos_to_id.read();
        vec.get(pos as usize)
            .and_then(|opt| opt.as_ref())
            .map(bytes_to_doc_id)
    }

    /// Remove a document from id_to_pos (position stays allocated; bitmap handles the hole).
    pub fn remove(&self, doc_id: &str) {
        if let Some(bytes) = doc_id_to_bytes(doc_id) {
            let pos = self.id_to_pos.write().remove(&bytes);
            if let Some(pos) = pos {
                let mut vec = self.pos_to_id.write();
                if let Some(slot) = vec.get_mut(pos as usize) {
                    *slot = None;
                }
            }
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
}

impl Default for AcceleratorConfig {
    fn default() -> Self {
        AcceleratorConfig {
            bitmap_fields: Vec::new(),
            max_cardinality: 1000,
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
    /// false during rebuild; queries fall back to full scan.
    ready: AtomicBool,
    /// Cardinality profiler for auto-detection.
    profiler: CardinalityProfiler,
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
        ScanAccelerator {
            columns,
            positions: RowPositionMap::new(),
            config: RwLock::new(config),
            ready: AtomicBool::new(false),
            profiler: CardinalityProfiler::new(has_fields),
        }
    }

    pub fn config_mut(&self) -> parking_lot::RwLockWriteGuard<'_, AcceleratorConfig> {
        self.config.write()
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
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

    /// Check if a rebuild is needed (auto-detection completed but accelerator not ready).
    #[allow(dead_code)]
    pub fn needs_rebuild(&self) -> bool {
        !self.is_ready() && self.has_columns() && self.profiler.is_done()
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
                let max_card = self.config.read().max_cardinality;
                let detected = self.profiler.analyze(max_card);
                if !detected.is_empty() {
                    let fields: Vec<String> = detected.iter().map(|(f, _)| f.clone()).collect();
                    let field_info: Vec<String> = detected
                        .iter()
                        .map(|(f, c)| format!("{f} ({c} values)"))
                        .collect();
                    info!(
                        fields = %field_info.join(", "),
                        "Scan accelerator: auto-detected bitmap fields"
                    );
                    let mut cols = self.columns.write();
                    for (field, _) in &detected {
                        if !cols.contains_key(field) {
                            cols.insert(field.clone(), BitmapColumn::new(field.clone()));
                        }
                    }
                    drop(cols);
                    self.config.write().bitmap_fields = fields;
                    // Note: The accelerator needs a full rebuild to populate
                    // the newly detected columns. This is handled externally
                    // (the caller checks needs_rebuild()).
                }
                self.profiler.finish();
            }
        }

        let columns = self.columns.read();
        let max_card = self.config.read().max_cardinality;

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
            }
            column.exists_bitmap.write().remove(pos);
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
        let max_card = self.config.read().max_cardinality;

        for (field_path, column) in columns.iter() {
            let old_val = resolve_json_path(old_doc, field_path).map(value_to_string_key);
            let new_val = resolve_json_path(new_doc, field_path).map(value_to_string_key);

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
        self.profiler.reset();
    }

    /// Rebuild the accelerator from all documents in storage.
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
                // Bitmap-accelerated $or narrowing:
                // If ALL children are bitmap-resolvable, return the union.
                // If SOME are resolvable, return partial bitmap + residual.
                // If NONE are resolvable, return None.
                let mut bitmap_result = RoaringBitmap::new();
                let mut non_bitmap_children: Vec<FilterNode> = Vec::new();
                let mut any_resolved = false;

                for child in children {
                    match self.bitmap_scan_inner(child) {
                        Some(BitmapScanResult {
                            bitmap,
                            residual_filter: None,
                        }) => {
                            bitmap_result |= &bitmap;
                            any_resolved = true;
                        }
                        Some(BitmapScanResult {
                            bitmap,
                            residual_filter: Some(_residual),
                        }) => {
                            // Child was partially resolved — can't cleanly union partial results
                            // for $or. Include the bitmap result but add original child as residual.
                            bitmap_result |= &bitmap;
                            non_bitmap_children.push(child.clone());
                            any_resolved = true;
                        }
                        None => {
                            non_bitmap_children.push(child.clone());
                        }
                    }
                }

                if !any_resolved {
                    return None;
                }

                if non_bitmap_children.is_empty() {
                    // Fully resolved via bitmaps
                    Some(BitmapScanResult {
                        bitmap: bitmap_result,
                        residual_filter: None,
                    })
                } else {
                    // Partial coverage — return what we have, executor handles the rest.
                    // TODO: Hybrid approach — for non-bitmap $or children, run mini
                    // full-scans excluding positions already in bitmap_result, then union.
                    // This avoids scanning 2.1M docs when most $or branches are
                    // bitmap-resolvable. See SCAN-ACCELERATOR-DESIGN.md section 4.4.
                    let residual = if non_bitmap_children.len() == 1 {
                        non_bitmap_children.into_iter().next().unwrap()
                    } else {
                        FilterNode::Or(non_bitmap_children)
                    };
                    Some(BitmapScanResult {
                        bitmap: bitmap_result,
                        residual_filter: Some(residual),
                    })
                }
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
        let columns = self.columns.read();
        let mut column_stats = Vec::new();
        for (field, column) in columns.iter() {
            column_stats.push(ColumnStat {
                field: field.clone(),
                cardinality: column.cardinality.load(Ordering::Relaxed),
                memory_bytes: column.memory_bytes(),
            });
        }
        AcceleratorStats {
            ready: self.is_ready(),
            total_positions: self.positions.len(),
            columns: column_stats,
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

        // Persist position map
        let positions_vec = self.positions.pos_to_id.read();
        let next_pos = self.positions.next_pos.load(Ordering::Relaxed);

        // Write positions metadata
        let meta = serde_json::json!({
            "next_pos": next_pos,
            "count": positions_vec.len(),
        });
        fs::write(
            bitmap_dir.join("positions.meta.json"),
            serde_json::to_string_pretty(&meta).unwrap_or_default(),
        )?;

        // Write position map as binary (16 bytes per entry, with 1 byte present flag)
        let mut pos_data = Vec::with_capacity(positions_vec.len() * 17);
        for slot in positions_vec.iter() {
            match slot {
                Some(bytes) => {
                    pos_data.push(1u8);
                    pos_data.extend_from_slice(bytes);
                }
                None => {
                    pos_data.push(0u8);
                    pos_data.extend_from_slice(&[0u8; 16]);
                }
            }
        }
        fs::write(bitmap_dir.join("positions.map.bin"), &pos_data)?;

        // Persist id_to_pos as well (for fast lookup reconstruction)
        let id_map = self.positions.id_to_pos.read();
        let mut id_data = Vec::with_capacity(id_map.len() * 20);
        for (bytes, pos) in id_map.iter() {
            id_data.extend_from_slice(bytes);
            id_data.extend_from_slice(&pos.to_le_bytes());
        }
        fs::write(bitmap_dir.join("positions.ids.bin"), &id_data)?;

        // Persist bitmap columns
        let columns = self.columns.read();
        let mut columns_meta = Vec::new();

        for (field_path, column) in columns.iter() {
            let bitmaps = column.value_bitmaps.read();
            let exists = column.exists_bitmap.read();

            // Safe field name for filesystem
            let safe_field = field_path.replace('.', "_DOT_");

            // Write exists bitmap
            let mut exists_bytes = Vec::new();
            exists
                .serialize_into(&mut exists_bytes)
                .map_err(std::io::Error::other)?;
            fs::write(
                bitmap_dir.join(format!("{safe_field}.exists.roaring")),
                &exists_bytes,
            )?;

            // Write each value bitmap
            let mut value_keys = Vec::new();
            for (i, (value_key, bitmap)) in bitmaps.iter().enumerate() {
                let mut bitmap_bytes = Vec::new();
                bitmap
                    .serialize_into(&mut bitmap_bytes)
                    .map_err(std::io::Error::other)?;
                let filename = format!("{safe_field}_v{i}.roaring");
                fs::write(bitmap_dir.join(&filename), &bitmap_bytes)?;
                value_keys.push((value_key.clone(), filename));
            }

            columns_meta.push(serde_json::json!({
                "field_path": field_path,
                "safe_field": safe_field,
                "cardinality": column.cardinality.load(Ordering::Relaxed),
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

        // Load position map
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
                let mut bytes = [0u8; 16];
                bytes.copy_from_slice(&pos_data[i..i + 16]);
                pos_vec.push(Some(bytes));
            } else {
                pos_vec.push(None);
            }
            i += 16;
        }

        // Load id_to_pos
        let id_data = match fs::read(bitmap_dir.join("positions.ids.bin")) {
            Ok(d) => d,
            Err(_) => return false,
        };

        let mut id_map = HashMap::new();
        let mut j = 0;
        while j + 20 <= id_data.len() {
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&id_data[j..j + 16]);
            let pos = u32::from_le_bytes([
                id_data[j + 16],
                id_data[j + 17],
                id_data[j + 18],
                id_data[j + 19],
            ]);
            id_map.insert(bytes, pos);
            j += 20;
        }

        // Install position data
        *self.positions.id_to_pos.write() = id_map;
        *self.positions.pos_to_id.write() = pos_vec;
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
    sample_target: u32,
    done: AtomicBool,
    /// If true, skip profiling (fields were explicitly configured)
    skip: bool,
}

impl CardinalityProfiler {
    pub fn new(skip: bool) -> Self {
        CardinalityProfiler {
            observed: RwLock::new(HashMap::new()),
            sample_count: AtomicU32::new(0),
            sample_target: 10_000,
            done: AtomicBool::new(skip),
            skip,
        }
    }

    #[allow(dead_code)]
    pub fn set_sample_target(&self, _target: u32) {
        // sample_target is set at construction time; this is a no-op placeholder
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

        if count >= self.sample_target {
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
