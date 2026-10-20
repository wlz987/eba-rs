use crate::prelude::*;
use alloc::sync::Arc;

use super::envelope::{make_envelope, Envelope, MakeEnvelope};
use super::header::{copy_header, Address, Header};
use super::ids::{ActorId, EnvelopeId};
use super::result::ResultBody;
use crate::errors::EnvelopeBuildError;

pub struct Matchmaker {
    topic: String,
    cause: EnvelopeId,
    correlation_id: EnvelopeId,
}

impl Matchmaker {
    pub fn new(request: &Header) -> Result<Self, crate::errors::BusError> {
        let header = copy_header(request);
        let Address::Request { reply_to } = &header.address else {
            return Err(crate::errors::BusError(
                "Matchmaker requires a request".into(),
            ));
        };
        Ok(Matchmaker {
            topic: reply_to.clone(),
            cause: header.cause,
            correlation_id: header.id,
        })
    }

    pub fn reply(
        &self,
        body: &ResultBody,
        from: ActorId,
        id: EnvelopeId,
    ) -> Result<Envelope, EnvelopeBuildError> {
        make_envelope(MakeEnvelope {
            topic: self.topic.clone(),
            payload: Arc::new(body.clone()),
            from,
            cause: Some(self.cause.clone()),
            id: Some(id),
            kind: "result".into(),
            reply_to: None,
            correlation_id: Some(self.correlation_id.clone()),
        })
    }
}
