use alloc::rc::Rc;

use crate::envelope::ids::ActorId;
use crate::envelope::{make_envelope, MakeEnvelope};
use crate::errors::{BoxError, BusError};
use crate::job::caller::{CallOptions, Caller};
use crate::job::host::JobHost;
use crate::mail::inbox::Inbox;
use crate::prelude::*;
use crate::runtime::pending::Pending;
use crate::Payload;

use super::Runtime;

impl Runtime {
    pub fn ask(
        &mut self,
        topic: &str,
        payload: Payload,
        from: ActorId,
        timeout_ms: Option<u64>,
    ) -> Result<Pending, BoxError> {
        let caller = match &self.front_caller {
            Some(caller) => caller.clone(),
            None => {
                let caller = Caller::with_actor(
                    Inbox::new(8).receiver(),
                    self.id_gen.clone(),
                    256,
                    crate::envelope::ids::actor_id("sys.ask"),
                );
                self.add(caller.clone() as Rc<dyn JobHost>)?;
                self.front_caller = Some(caller.clone());
                caller
            }
        };
        let envelope = caller.admit(CallOptions {
            topic: topic.into(),
            payload,
            from,
            timeout_ms,
        })?;
        Ok(Pending::new(envelope))
    }

    pub fn tell(&mut self, topic: &str, payload: Payload, from: ActorId) -> Result<(), BoxError> {
        let id = self.id_gen.borrow_mut().next_envelope_id();
        let event = make_envelope(MakeEnvelope {
            topic: topic.into(),
            payload,
            from,
            cause: None,
            id: Some(id),
            kind: "event".into(),
            reply_to: None,
            correlation_id: None,
        })?;
        if !self.bus.publish(&Rc::new(event))? {
            return Err(Box::from(BusError("event not published".into())));
        }
        Ok(())
    }
}
