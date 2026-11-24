//! 值表示。线上传播、结构化、可确定性比较。
use crate::prelude::*;
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
    Seq(Vec<Value>),
    Map(BTreeMap<String, Value>),
}
impl Value {
    pub fn as_map(&self) -> Option<&BTreeMap<String, Value>> {
        match self {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(bytes) => Some(bytes.as_slice()),
            _ => None,
        }
    }
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.as_map().and_then(|m| m.get(key))
    }
}
pub fn ok_payload(value: Value) -> Value {
    let mut m = BTreeMap::new();
    m.insert("ok".into(), Value::Bool(true));
    m.insert("value".into(), value);
    Value::Map(m)
}
pub fn err_payload(error: impl Into<String>) -> Value {
    err_payload_code(error, None)
}
pub fn err_payload_code(error: impl Into<String>, code: Option<i64>) -> Value {
    let mut m = BTreeMap::new();
    m.insert("ok".into(), Value::Bool(false));
    m.insert("error".into(), Value::Str(error.into()));
    if let Some(c) = code {
        m.insert("code".into(), Value::Int(c));
    }
    Value::Map(m)
}
/// 展开 detail 字段到错误体。
pub fn err_payload_detail(error: impl Into<String>, detail: Value) -> Value {
    let mut m = match err_payload(error) {
        Value::Map(m) => m,
        _ => unreachable!(),
    };
    if let Some(extra) = detail.as_map() {
        for (k, v) in extra {
            m.insert(k.clone(), v.clone());
        }
    }
    Value::Map(m)
}
/// 展开 detail 但 code 覆盖 detail.code。
pub fn err_payload_code_detail(error: impl Into<String>, code: i64, detail: Value) -> Value {
    let mut body = err_payload_detail(error, detail);
    if let Value::Map(ref mut m) = body {
        m.insert("code".into(), Value::Int(code)); // 后写，保证 code 胜出
    }
    body
}
pub fn is_result_ok(body: &Value) -> bool {
    matches!(
        body.as_map().and_then(|m| m.get("ok")),
        Some(Value::Bool(true))
    )
}
pub fn is_result_err(body: &Value) -> bool {
    matches!(
        body.as_map().and_then(|m| m.get("ok")),
        Some(Value::Bool(false))
    )
}
pub fn result_value(body: &Value) -> Option<&Value> {
    body.as_map()?.get("value")
}
pub fn result_error(body: &Value) -> Option<&str> {
    match body.as_map()?.get("error")? {
        Value::Str(s) => Some(s.as_str()),
        _ => None,
    }
}
pub fn has_result_shape(body: &Value) -> bool {
    is_result_ok(body) || is_result_err(body)
}
