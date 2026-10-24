use crate::prelude::*;
use alloc::rc::Rc;

use crate::envelope::ids::ActorId;
use crate::envelope::{make_envelope, Envelope, MakeEnvelope};
use crate::errors::{BoxError, BusError};
use crate::job::caller::{Call, CallOptions, Caller, Pay};
use crate::job::host::JobHost;
use crate::mail::inbox::Inbox;
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
        let envelope = self.front_caller()?.admit(CallOptions {
            topic: topic.into(),
            payload,
            from,
            timeout_ms,
        })?;
        Ok(Pending::new(envelope))
    }

    /// Hand the completion to `pay`; it runs once inside a turn and must not re-enter the runtime.
    pub fn ask_with(
        &mut self,
        topic: &str,
        payload: Payload,
        from: ActorId,
        timeout_ms: Option<u64>,
        pay: Pay,
    ) -> Result<Rc<Envelope>, BoxError> {
        let envelope = self.front_caller()?.admit(CallOptions {
            topic: topic.into(),
            payload,
            from,
            timeout_ms,
        })?;
        let Some(call) = envelope.payload.downcast_ref::<Call>() else {
            return Err(Box::from(BusError("admitted root has no call".into())));
        };
        call.set_pay(pay);
        Ok(envelope)
    }

    fn front_caller(&mut self) -> Result<Rc<Caller>, BoxError> {
        if let Some(caller) = &self.front_caller {
            return Ok(caller.clone());
        }
        // front inbox matches the caller active limit
        let caller = Caller::with_actor(
            Inbox::new(256).receiver(),
            self.id_gen.clone(),
            256,
            crate::envelope::ids::actor_id("sys.ask"),
        );
        self.add(caller.clone() as Rc<dyn JobHost>)?;
        self.front_caller = Some(caller.clone());
        Ok(caller)
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
