use std::sync::Arc;

use super::header::{Address, Header};
use super::idgen::IdGen;
use super::ids::{ActorId, EnvelopeId};
use super::result::ResultBody;
use crate::errors::EnvelopeBuildError;
use crate::Payload;

#[derive(Debug, Clone)]
pub struct Envelope {
    pub header: Header,
    pub payload: Payload,
}

pub struct MakeEnvelope {
    pub topic: String,
    pub payload: Payload,
    pub from: ActorId,
    pub cause: Option<EnvelopeId>,
    pub id: Option<EnvelopeId>,
    pub kind: String,
    pub reply_to: Option<String>,
    pub correlation_id: Option<EnvelopeId>,
}

impl MakeEnvelope {
    pub fn event(topic: impl Into<String>, payload: Payload, from: ActorId) -> Self {
        MakeEnvelope {
            topic: topic.into(),
            payload,
            from,
            cause: None,
            id: None,
            kind: "event".into(),
            reply_to: None,
            correlation_id: None,
        }
    }
}

pub fn make_envelope(request: MakeEnvelope) -> Result<Envelope, EnvelopeBuildError> {
    let assigned = match &request.id {
        Some(id) if !id.0.is_empty() => id.clone(),
        Some(_) => return Err(EnvelopeBuildError("id must be a nonempty string".into())),
        None => {
            return Err(EnvelopeBuildError(
                "id_gen is required when id is omitted".into(),
            ))
        }
    };
    let cause = request.cause.clone().unwrap_or_else(|| assigned.clone());
    if cause.0.is_empty() {
        return Err(EnvelopeBuildError("cause must be a nonempty string".into()));
    }
    let address = match request.kind.as_str() {
        "request" => {
            if request.correlation_id.is_some() {
                return Err(EnvelopeBuildError(
                    "request cannot have correlation_id".into(),
                ));
            }
            let reply_to = request
                .reply_to
                .ok_or_else(|| EnvelopeBuildError("request requires reply_to".into()))?;
            Address::Request { reply_to }
        }
        "result" => {
            if request.reply_to.is_some() {
                return Err(EnvelopeBuildError(
                    "result requires only correlation_id".into(),
                ));
            }
            let correlation_id = request
                .correlation_id
                .ok_or_else(|| EnvelopeBuildError("result requires only correlation_id".into()))?;
            if correlation_id.0.is_empty() {
                return Err(EnvelopeBuildError(
                    "result requires only correlation_id".into(),
                ));
            }
            Address::Result { correlation_id }
        }
        "event" => {
            if request.reply_to.is_some() || request.correlation_id.is_some() {
                return Err(EnvelopeBuildError(
                    "invalid message kind or protocol address".into(),
                ));
            }
            Address::Event
        }
        "diagnostic" => {
            if request.reply_to.is_some() || request.correlation_id.is_some() {
                return Err(EnvelopeBuildError(
                    "invalid message kind or protocol address".into(),
                ));
            }
            Address::Diagnostic
        }
        _ => {
            return Err(EnvelopeBuildError(
                "invalid message kind or protocol address".into(),
            ))
        }
    };
    let header = Header::new(assigned, request.topic, request.from, cause, address)?;
    Ok(Envelope {
        header,
        payload: request.payload,
    })
}

pub fn snapshot_envelope(envelope: Arc<Envelope>) -> Arc<Envelope> {
    envelope
}

pub type IdGenBox = Box<dyn IdGen>;
pub type ResultBox = Arc<ResultBody>;
