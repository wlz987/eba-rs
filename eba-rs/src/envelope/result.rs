use std::collections::HashMap;

use super::ids::EnvelopeId;
use super::Envelope;
use crate::errors::EnvelopeBuildError;
use crate::Payload;

#[derive(Debug, Clone)]
pub enum ResultBody {
    Ok {
        value: Payload,
    },
    Err {
        error: String,
        detail: HashMap<String, Payload>,
    },
}

#[derive(Debug, Clone)]
pub struct Completion {
    pub request_id: EnvelopeId,
    pub body: Option<ResultBody>,
}

impl Completion {
    pub fn is_timeout(&self) -> bool {
        self.body.is_none()
    }
}

pub fn ok_payload(value: Payload) -> ResultBody {
    ResultBody::Ok { value }
}

pub fn err_payload(
    error: impl Into<String>,
    detail: Option<HashMap<String, Payload>>,
) -> Result<ResultBody, EnvelopeBuildError> {
    let extra = detail.unwrap_or_default();
    for key in extra.keys() {
        if matches!(key.as_str(), "ok" | "error" | "value") {
            return Err(EnvelopeBuildError(format!(
                "reserved result key: {:?}",
                key
            )));
        }
    }
    Ok(ResultBody::Err {
        error: error.into(),
        detail: extra,
    })
}

pub fn is_result_ok(body: &ResultBody) -> bool {
    matches!(body, ResultBody::Ok { .. })
}

pub fn is_result_err(body: &ResultBody) -> bool {
    matches!(body, ResultBody::Err { .. })
}

pub fn has_result_shape(body: &dyn std::any::Any) -> bool {
    body.downcast_ref::<ResultBody>().is_some()
}

pub fn result_value(body: &ResultBody) -> Option<&Payload> {
    match body {
        ResultBody::Ok { value } => Some(value),
        ResultBody::Err { .. } => None,
    }
}

pub fn result_error(body: &ResultBody) -> Option<&str> {
    match body {
        ResultBody::Err { error, .. } => Some(error),
        ResultBody::Ok { .. } => None,
    }
}

pub fn result_body(envelope: &Envelope) -> Option<&ResultBody> {
    if envelope.header.address.kind() != "result" {
        return None;
    }
    envelope.payload.downcast_ref::<ResultBody>()
}

pub fn result_request_id(envelope: &Envelope) -> Option<EnvelopeId> {
    envelope.header.address.correlation_id().cloned()
}
