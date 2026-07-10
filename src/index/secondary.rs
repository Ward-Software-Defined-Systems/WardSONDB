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
