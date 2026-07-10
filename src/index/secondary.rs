use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub collection: String,
    /// Fields covered by this index. Single-field indexes have one entry;
    /// compound indexes have multiple fields in order.
    #[serde(default)]
    pub fields: Vec<String>,
    pub created_at: String,
    /// Backward-compat: single-field indexes also expose `field`.
    /// For compound indexes this is the first field.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub field: String,
}

impl IndexDef {
    /// Create a new index definition.
    pub fn new(name: String, collection: String, fields: Vec<String>, created_at: String) -> Self {
        let field = fields.first().cloned().unwrap_or_default();
        IndexDef {
            name,
            collection,
            fields,
            field,
            created_at,
        }
    }

    /// Whether this is a compound (multi-field) index.
    pub fn is_compound(&self) -> bool {
        self.fields.len() > 1
    }
}

/// Encode a JSON value into bytes that sort lexicographically in the correct order.
///
/// Encoding scheme (type prefix byte ensures cross-type ordering):
///   0x00 = null
///   0x01 = false, 0x02 = true
///   0x03 = number (IEEE 754 with sign-flip for correct ordering)
///   0x04 = string (raw UTF-8 bytes)
///   0x05 = array/object (serialized JSON text)
///
/// Cross-type order: null < false < true < number < string < array/object.
/// `compare_values_total` below must stay byte-for-byte consistent with this
/// encoding — the lockstep test in this file enforces it.
pub fn value_to_sortable_bytes(value: &Value) -> Vec<u8> {
    match value {
        Value::Null => vec![0x00],
        Value::Bool(false) => vec![0x01],
        Value::Bool(true) => vec![0x02],
        Value::Number(n) => {
            let f = n.as_f64().unwrap_or(0.0);
            let mut bytes = vec![0x03];
            let bits = f.to_bits();
            // Flip sign bit for positive numbers; flip all bits for negative
            let sortable = if f.is_sign_negative() {
                !bits
            } else {
                bits ^ (1u64 << 63)
            };
            bytes.extend_from_slice(&sortable.to_be_bytes());
            bytes
        }
        Value::String(s) => {
            let mut bytes = vec![0x04];
            bytes.extend_from_slice(s.as_bytes());
            bytes
        }
        // Arrays/objects: serialize to JSON string for consistent ordering
        other => {
            let mut bytes = vec![0x05];
            bytes.extend_from_slice(serde_json::to_string(other).unwrap_or_default().as_bytes());
            bytes
        }
    }
}

/// The encoding's type prefix byte for a value (see `value_to_sortable_bytes`).
fn type_prefix_byte(v: &Value) -> u8 {
    match v {
        Value::Null => 0x00,
        Value::Bool(false) => 0x01,
        Value::Bool(true) => 0x02,
        Value::Number(_) => 0x03,
        Value::String(_) => 0x04,
        Value::Array(_) | Value::Object(_) => 0x05,
    }
}

/// Total order over JSON values, byte-for-byte identical to the order of
/// `value_to_sortable_bytes` output (the lockstep test below enforces this).
/// This is the database's ONE collation for ordering surfaces: /query sort,
/// cursor positions, aggregate $sort, $min/$max, $collect. Range FILTERS
/// deliberately do not use it — see `query::filter::compare_values`.
pub fn compare_values_total(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        // total_cmp is the same transform as the encoding's sign-flip bit
        // order for every f64, including -0.0 < 0.0; as_f64 mirrors the
        // encoding's lossy conversion (>2^53 collisions stay consistent).
        (Value::Number(x), Value::Number(y)) => x
            .as_f64()
            .unwrap_or(0.0)
            .total_cmp(&y.as_f64().unwrap_or(0.0)),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        // Arrays/objects share prefix 0x05 and order by serialized JSON text.
        // This arm must catch (Array, Object) pairs too — '[' < '{' — so they
        // never reach the prefix-byte arm and compare as a spurious Equal.
        (Value::Array(_) | Value::Object(_), Value::Array(_) | Value::Object(_)) => {
            serde_json::to_string(a)
                .unwrap_or_default()
                .cmp(&serde_json::to_string(b).unwrap_or_default())
        }
        // Cross-bucket: only differing prefix bytes reach here (every
        // same-bucket pair is handled above), so this never returns Equal.
        _ => type_prefix_byte(a).cmp(&type_prefix_byte(b)),
    }
}

/// Build an index key for a single-field index: {encoded_value}\x00{doc_id}
pub fn make_index_key(value: &Value, doc_id: &str) -> Vec<u8> {
    let mut key = value_to_sortable_bytes(value);
    key.push(0x00);
    key.extend_from_slice(doc_id.as_bytes());
    key
}

/// Build an index key for a compound index: {encoded_v1}\x01{encoded_v2}\x01...\x00{doc_id}
/// Uses \x01 as field separator (distinct from \x00 doc_id separator).
pub fn make_compound_index_key(values: &[&Value], doc_id: &str) -> Vec<u8> {
    let mut key = Vec::new();
    for (i, value) in values.iter().enumerate() {
        if i > 0 {
            key.push(0x01); // field separator
        }
        key.extend_from_slice(&value_to_sortable_bytes(value));
    }
    key.push(0x00); // doc_id separator
    key.extend_from_slice(doc_id.as_bytes());
    key
}

/// Smallest byte string greater than every key that has `prefix` as a
/// prefix — the exclusive upper bound of a prefix window, or the inclusive
/// start that skips exactly that prefix's keys. Planner-built prefixes always
/// end with a 0x00/0x01 separator, so in practice this bumps that byte; the
/// loop handles trailing 0xFF for generality. An all-0xFF prefix has no
/// finite successor (unreachable here), so a maximal sentinel keeps the
/// function total.
pub fn prefix_successor(prefix: &[u8]) -> Vec<u8> {
    let mut p = prefix.to_vec();
    while let Some(&last) = p.last() {
        if last == 0xFF {
            p.pop();
        } else {
            *p.last_mut().unwrap() = last + 1;
            return p;
        }
    }
    vec![0xFF; prefix.len() + 1]
}

/// Decode sortable bytes back into a JSON value (inverse of value_to_sortable_bytes).
pub fn decode_sortable_bytes(bytes: &[u8]) -> Option<Value> {
    if bytes.is_empty() {
        return None;
    }
    match bytes[0] {
        0x00 => Some(Value::Null),
        0x01 => Some(Value::Bool(false)),
        0x02 => Some(Value::Bool(true)),
        0x03 => {
            if bytes.len() < 9 {
                return None;
            }
            let mut be = [0u8; 8];
            be.copy_from_slice(&bytes[1..9]);
            let sortable = u64::from_be_bytes(be);
            let bits = if sortable & (1u64 << 63) != 0 {
                sortable ^ (1u64 << 63) // positive: flip sign bit back
            } else {
                !sortable // negative: flip all bits back
            };
            let f = f64::from_bits(bits);
            serde_json::Number::from_f64(f).map(Value::Number)
        }
        0x04 => {
            let s = std::str::from_utf8(&bytes[1..]).ok()?;
            Some(Value::String(s.to_string()))
        }
        0x05 => {
            let s = std::str::from_utf8(&bytes[1..]).ok()?;
            serde_json::from_str(s).ok()
        }
        _ => None,
    }
}

/// Extract the doc_id from an index key by splitting on the \x00 separator.
/// The doc_id is everything after the last \x00.
pub fn extract_doc_id_from_key(key: &[u8]) -> Option<String> {
    // Key format: {encoded_value}\x00{doc_id}
    // Find the last \x00 separator (doc_id is guaranteed to not contain \x00)
    let sep_pos = key.iter().rposition(|&b| b == 0x00)?;
    if sep_pos + 1 >= key.len() {
        return None;
    }
    String::from_utf8(key[sep_pos + 1..].to_vec()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Corpus spanning every encoding bucket plus the known edge values:
    /// signed zeros, negative/fractional numbers, the 2^53/2^53+1 lossy
    /// collision, empty string/array/object, nesting, '[' vs '{' text order.
    fn corpus() -> Vec<Value> {
        vec![
            json!(null),
            json!(false),
            json!(true),
            json!(-3.5),
            json!(-0.0),
            json!(0.0),
            json!(1),
            json!(1.5),
            json!(42),
            json!(9007199254740992u64), // 2^53
            json!(9007199254740993u64), // 2^53 + 1: as_f64-collides with 2^53
            json!(""),
            json!("A"),
            json!("a"),
            json!("ab"),
            json!("z"),
            json!([]),
            json!([1]),
            json!([1, 2]),
            json!(["a"]),
            json!({}),
            json!({"a": 1}),
            json!({"a": {"b": [1]}}),
            json!({"b": 1}),
        ]
    }

    /// THE invariant of this module: the in-memory comparator and the key
    /// encoding define the same order, for every ordered pair. Any future
    /// change to `value_to_sortable_bytes` or `compare_values_total` that
    /// breaks byte parity fails here.
    #[test]
    fn compare_values_total_matches_encoding_byte_order() {
        let values = corpus();
        for a in &values {
            for b in &values {
                assert_eq!(
                    compare_values_total(a, b),
                    value_to_sortable_bytes(a).cmp(&value_to_sortable_bytes(b)),
                    "comparator vs encoding disagree for {a} vs {b}"
                );
            }
        }
    }

    /// The property whose absence caused R2: transitivity across buckets.
    #[test]
    fn total_order_transitive_on_mixed_sample() {
        let values = corpus();
        for a in &values {
            for b in &values {
                for c in &values {
                    use std::cmp::Ordering::Greater;
                    if compare_values_total(a, b) != Greater
                        && compare_values_total(b, c) != Greater
                    {
                        assert_ne!(
                            compare_values_total(a, c),
                            Greater,
                            "intransitive: {a} <= {b} <= {c} but {a} > {c}"
                        );
                    }
                }
            }
        }
    }
}
