use crate::envelope::{Envelope, EnvelopeId, Value};
use std::collections::BTreeMap;
use std::format;

const RESERVED: [&str; 3] = ["ok", "error", "value"];

pub fn ok_payload(value: impl Into<Value>) -> Value {
    let mut m = BTreeMap::new();
    m.insert("ok".into(), Value::Bool(true));
    m.insert("value".into(), value.into());
    Value::Object(m)
}

pub fn err_payload(error: &str, detail: &[(&str, Value)]) -> Result<Value, crate::Error> {
    let mut m = BTreeMap::new();
    m.insert("ok".into(), Value::Bool(false));
    m.insert("error".into(), Value::Str(error.into()));
    for (k, v) in detail {
        if RESERVED.contains(k) {
            return Err(crate::Error::State(format!("reserved result key: {k:?}")));
        }
        m.insert((*k).to_string(), (*v).clone());
    }
    Ok(Value::Object(m))
}

fn as_object(v: &Value) -> Option<&BTreeMap<String, Value>> {
    match v {
        Value::Object(m) => Some(m),
        _ => None,
    }
}

pub fn is_result_ok(body: &Value) -> bool {
    match as_object(body) {
        Some(m) => matches!(m.get("ok"), Some(Value::Bool(true))) && m.contains_key("value"),
        None => false,
    }
}

pub fn is_result_err(body: &Value) -> bool {
    match as_object(body) {
        Some(m) => {
            matches!(m.get("ok"), Some(Value::Bool(false)))
                && matches!(m.get("error"), Some(Value::Str(_)))
        }
        None => false,
    }
}

pub(crate) fn has_result_shape(body: &Value) -> bool {
    is_result_ok(body) || is_result_err(body)
}

pub fn result_value(body: &Value) -> Option<&Value> {
    if !is_result_ok(body) {
        return None;
    }
    body.get("value")
}

pub fn result_error(body: &Value) -> Option<&str> {
    if !is_result_err(body) {
        return None;
    }
    body.get("error").and_then(Value::as_str)
}

pub fn result_body(env: &Envelope) -> Option<&Value> {
    let raw = env.payload.get("result")?;
    if !has_result_shape(raw) {
        return None;
    }
    Some(raw)
}

pub fn result_request_id(env: &Envelope) -> Option<EnvelopeId> {
    match env.payload.get("request_id") {
        Some(Value::Str(s)) => Some(EnvelopeId(s.clone())),
        _ => None,
    }
}
