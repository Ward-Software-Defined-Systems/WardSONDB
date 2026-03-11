use serde_json::Value;

use super::filter::resolve_json_path;

#[derive(Debug, Clone)]
pub struct SortField {
    pub field: String,
    pub ascending: bool,
}

pub fn parse_sort(sort_spec: &[Value]) -> Vec<SortField> {
    let mut fields = Vec::new();
    for item in sort_spec {
        if let Value::Object(obj) = item {
            for (field, direction) in obj {
                let ascending = !matches!(direction.as_str(), Some("desc"));
                fields.push(SortField {
                    field: field.clone(),
                    ascending,
                });
            }
        }
    }
    fields
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
