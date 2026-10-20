use super::ids::{ActorId, EnvelopeId};
use super::topic::split_topic;
use crate::errors::EnvelopeBuildError;
use crate::prelude::*;

#[derive(Debug, Clone)]
pub enum Address {
    Event,
    Diagnostic,
    Request { reply_to: String },
    Result { correlation_id: EnvelopeId },
}

impl Address {
    pub fn kind(&self) -> &'static str {
        match self {
            Address::Event => "event",
            Address::Diagnostic => "diagnostic",
            Address::Request { .. } => "request",
            Address::Result { .. } => "result",
        }
    }
    pub fn reply_to(&self) -> Option<&str> {
        match self {
            Address::Request { reply_to } => Some(reply_to),
            _ => None,
        }
    }
    pub fn correlation_id(&self) -> Option<&EnvelopeId> {
        match self {
            Address::Result { correlation_id } => Some(correlation_id),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Header {
    pub id: EnvelopeId,
    pub topic: String,
    pub from: ActorId,
    pub cause: EnvelopeId,
    pub address: Address,
}

impl Header {
    pub fn new(
        id: EnvelopeId,
        topic: String,
        from: ActorId,
        cause: EnvelopeId,
        address: Address,
    ) -> Result<Self, EnvelopeBuildError> {
        if id.0.is_empty() {
            return Err(EnvelopeBuildError("id must be a nonempty string".into()));
        }
        if cause.0.is_empty() {
            return Err(EnvelopeBuildError("cause must be a nonempty string".into()));
        }
        split_topic(&topic)?;
        match &address {
            Address::Request { reply_to } => {
                split_topic(reply_to)?;
            }
            Address::Result { correlation_id } => {
                if correlation_id.0.is_empty() {
                    return Err(EnvelopeBuildError("correlation_id must be nonempty".into()));
                }
            }
            _ => {}
        }
        Ok(Header {
            id,
            topic,
            from,
            cause,
            address,
        })
    }

    pub fn copy(&self) -> Self {
        Header {
            id: self.id.clone(),
            topic: self.topic.clone(),
            from: self.from.clone(),
            cause: self.cause.clone(),
            address: self.address.clone(),
        }
    }
}

pub fn copy_header(header: &Header) -> Header {
    header.copy()
}
