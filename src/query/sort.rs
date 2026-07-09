use serde_json::Value;

use super::filter::resolve_json_path;

#[derive(Debug, Clone)]
pub struct SortField {
    pub field: String,
    pub ascending: bool,
}

/// Parse a sort specification. Shared by the `/query` endpoint's `sort` field
/// and the aggregate `$sort` stage so both accept identical shapes:
///
/// - array form: `[{"field": dir}, ...]` — one field per element, priority in
///   array order (`[]` is a no-op)
/// - flat object form: `{"field": dir}` — exactly one field; multiple fields
///   are rejected because JSON object key order is not preserved after parsing
///
/// Directions: `"asc"`, `"desc"`, `1`, `-1` (also `1.0` / `-1.0`). Anything
/// else is an error naming the offending field.
///
/// Error messages are phrased to read naturally after the callers' prefixes
/// ("sort ..." / "Stage {i}: $sort ...").
pub fn parse_sort_spec(spec: &Value) -> Result<Vec<SortField>, String> {
    match spec {
        Value::Array(items) => {
            let mut fields = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                let Value::Object(obj) = item else {
                    return Err(format!(
                        "element {i} must be an object with exactly one field"
                    ));
                };
                if obj.len() != 1 {
                    return Err(format!(
                        "element {i} must have exactly one field (got {}); use one object per field: [{{\"a\": \"asc\"}}, {{\"b\": \"desc\"}}]",
                        obj.len()
                    ));
                }
                let (field, direction) = obj.iter().next().unwrap();
                fields.push(SortField {
                    field: field.clone(),
                    ascending: parse_direction(field, direction)?,
                });
            }
            Ok(fields)
        }
        Value::Object(obj) => match obj.len() {
            1 => {
                let (field, direction) = obj.iter().next().unwrap();
                Ok(vec![SortField {
                    field: field.clone(),
                    ascending: parse_direction(field, direction)?,
                }])
            }
            0 => Err("must not be an empty object".to_string()),
            n => Err(format!(
                "object with {n} fields is ambiguous (JSON object key order is not preserved); use the array form: [{{\"a\": \"asc\"}}, {{\"b\": \"desc\"}}]"
            )),
        },
        _ => Err("must be an array of single-field objects or a single-field object".to_string()),
    }
}

fn parse_direction(field: &str, direction: &Value) -> Result<bool, String> {
    let invalid = || {
        format!(
            "direction for field '{field}' must be \"asc\", \"desc\", 1, or -1 (got {})",
            serde_json::to_string(direction).unwrap_or_else(|_| "?".to_string())
        )
    };
    match direction {
        Value::String(s) if s == "asc" => Ok(true),
        Value::String(s) if s == "desc" => Ok(false),
        // as_f64 covers integer and float encodings; 1.0 and -1.0 are exact.
        Value::Number(n) => match n.as_f64() {
            Some(1.0) => Ok(true),
            Some(-1.0) => Ok(false),
            _ => Err(invalid()),
        },
        _ => Err(invalid()),
    }
}

pub fn sort_documents(docs: &mut [Value], sort_fields: &[SortField]) {
    docs.sort_by(|a, b| {
        for sf in sort_fields {
            let va = resolve_json_path(a, &sf.field);
            let vb = resolve_json_path(b, &sf.field);

            let ordering = compare_json_values(va, vb);
            let ordering = if sf.ascending {
                ordering
            } else {
                ordering.reverse()
            };

            if ordering != std::cmp::Ordering::Equal {
                return ordering;
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn compare_json_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(a), Some(b)) => compare_values(a, b),
    }
}

fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Number(a), Value::Number(b)) => {
            let af = a.as_f64().unwrap_or(0.0);
            let bf = b.as_f64().unwrap_or(0.0);
            af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal)
        }
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        _ => std::cmp::Ordering::Equal,
    }
}
