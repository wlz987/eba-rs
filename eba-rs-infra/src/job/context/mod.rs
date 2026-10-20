use crate::prelude::*;
use alloc::rc::Rc;
use core::cell::RefCell;

use crate::envelope::result::{has_result_shape, ResultBody};
use crate::envelope::{make_envelope, Envelope, EnvelopeId, MakeEnvelope};
use crate::errors::{BackpressureError, BoxError, BusError};
use crate::job::job::{Control, Intent, Lifecycle, SharedIdGen, State};
use crate::Payload;

mod commit;

pub use self::commit::{seal_context, Commit};

pub struct Buffer {
    pub job: Rc<RefCell<State>>,
    pub actor_id: crate::envelope::ids::ActorId,
    pub reply_to: String,
    pub id_gen: Option<SharedIdGen>,
    pub pending_count: usize,
    pub busy: bool,
    pub requests: Vec<Intent>,
    pub reply: Option<ResultBody>,
    pub control: Option<Control>,
}

pub struct Context {
    buffer: RefCell<Buffer>,
}

impl Context {
    pub fn new(
        job: Rc<RefCell<State>>,
        actor_id: crate::envelope::ids::ActorId,
        reply_to: String,
        id_gen: SharedIdGen,
        pending_count: usize,
    ) -> Self {
        Context {
            buffer: RefCell::new(Buffer {
                job,
                actor_id,
                reply_to,
                id_gen: Some(id_gen),
                pending_count,
                busy: false,
                requests: Vec::new(),
                reply: None,
                control: None,
            }),
        }
    }

    pub fn root(&self) -> Rc<Envelope> {
        self.buffer.borrow().job.borrow().root.clone()
    }

    pub fn request(
        &self,
        stage: &str,
        topic: &str,
        payload: Payload,
        timeout: Option<u64>,
    ) -> Result<EnvelopeId, BoxError> {
        let mut buffer = self.buffer.borrow_mut();
        if buffer.id_gen.is_none() {
            return Err(Box::from(BusError(
                "Context is valid only during its callback".into(),
            )));
        }
        if buffer.busy {
            return Err(Box::from(BusError("reentrant Context mutation".into())));
        }
        if buffer.control.is_some() || buffer.job.borrow().lifecycle != Lifecycle::Active {
            return Err(Box::from(BusError("job already finished".into())));
        }
        buffer.busy = true;
        let outcome = request_intent(&mut buffer, stage, topic, payload, timeout);
        buffer.busy = false;
        outcome
    }

    pub fn reply(&self, body: ResultBody) -> Result<(), BoxError> {
        self.mutate(|buffer| put_reply(buffer, body))
    }

    pub fn finish(&self, body: Option<ResultBody>) -> Result<(), BoxError> {
        self.mutate(|buffer| finish_control(buffer, body))
    }

    pub fn finished(&self) -> bool {
        let buffer = self.buffer.borrow();
        buffer.control.is_some() || buffer.job.borrow().lifecycle == Lifecycle::Finished
    }

    fn mutate<F>(&self, action: F) -> Result<(), BoxError>
    where
        F: FnOnce(&mut Buffer) -> Result<(), BoxError>,
    {
        let mut buffer = self.buffer.borrow_mut();
        if buffer.id_gen.is_none() {
            return Err(Box::from(BusError(
                "Context is valid only during its callback".into(),
            )));
        }
        if buffer.busy {
            return Err(Box::from(BusError("reentrant Context mutation".into())));
        }
        buffer.busy = true;
        let outcome = action(&mut buffer);
        buffer.busy = false;
        outcome
    }
}

fn request_intent(
    buffer: &mut Buffer,
    stage: &str,
    topic: &str,
    payload: Payload,
    timeout: Option<u64>,
) -> Result<EnvelopeId, BoxError> {
    {
        let job = buffer.job.borrow();
        if buffer.pending_count + buffer.requests.len() >= job.max_inflight {
            return Err(Box::from(BackpressureError("max_inflight".into())));
        }
        let cause = job.root.header.cause.clone();
        drop(job);
        let id = buffer
            .id_gen
            .as_ref()
            .unwrap()
            .borrow_mut()
            .next_envelope_id();
        let request = make_envelope(MakeEnvelope {
            topic: topic.into(),
            payload,
            from: buffer.actor_id.clone(),
            cause: Some(cause),
            id: Some(id),
            kind: "request".into(),
            reply_to: Some(buffer.reply_to.clone()),
            correlation_id: None,
        })
        .map_err(|error| Box::new(error) as BoxError)?;
        let returned = request.header.id.clone();
        buffer.requests.push(Intent {
            stage: stage.into(),
            request: Rc::new(request),
            timeout,
        });
        Ok(returned)
    }
}

fn put_reply(buffer: &mut Buffer, body: ResultBody) -> Result<(), BoxError> {
    let root = buffer.job.borrow().root.clone();
    if root.header.address.kind() != "request" {
        return Err(Box::from(BusError("root is not a request".into())));
    }
    if buffer.reply.is_some() || buffer.job.borrow().replied {
        return Err(Box::from(BusError("root already replied".into())));
    }
    if !has_result_shape(&body) {
        return Err(Box::from(BusError("invalid result body".into())));
    }
    buffer.reply = Some(body);
    Ok(())
}

fn finish_control(buffer: &mut Buffer, body: Option<ResultBody>) -> Result<(), BoxError> {
    if buffer.control.is_some() {
        return Ok(());
    }
    if let Some(chosen) = &body {
        let root = buffer.job.borrow().root.clone();
        if root.header.address.kind() == "request" {
            put_reply(buffer, chosen.clone())?;
        } else if !has_result_shape(chosen) {
            return Err(Box::from(BusError("invalid result body".into())));
        }
    }
    buffer.control = Some(Control { body });
    Ok(())
}
