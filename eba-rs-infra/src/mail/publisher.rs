use crate::prelude::*;
use alloc::rc::Rc;
use alloc::sync::Arc;

use crate::envelope::idgen::IdGen;
use crate::envelope::ids::{actor_id, ActorId};
use crate::envelope::{make_envelope, Envelope, MakeEnvelope};
use crate::errors::{BoxError, DeliveryError};
use crate::mail::bus::Bus;
use crate::Payload;

pub struct Publisher {
    pub actor_id: ActorId,
}

impl Publisher {
    pub fn new(actor: impl Into<String>) -> Self {
        Publisher {
            actor_id: actor_id(actor.into()),
        }
    }

    pub fn emit(
        &self,
        bus: &Bus,
        topic: &str,
        payload: Payload,
        id_gen: &mut dyn IdGen,
    ) -> Result<Rc<Envelope>, BoxError> {
        let id = id_gen.next_envelope_id();
        let envelope = make_envelope(MakeEnvelope {
            topic: topic.into(),
            payload,
            from: self.actor_id.clone(),
            cause: None,
            id: Some(id),
            kind: "event".into(),
            reply_to: None,
            correlation_id: None,
        })
        .map_err(|e| Box::from(e) as BoxError)?;
        let rc = Rc::new(envelope);
        if let Err(error) = bus.publish(&rc) {
            let delivery =
                DeliveryError::caused(Arc::new((*rc).clone()), "publication failed", error);
            return Err(Box::from(delivery));
        }
        Ok(rc)
    }
}
