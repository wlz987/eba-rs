use crate::prelude::*;
use alloc::rc::Rc;
use alloc::string::String;
use core::cell::RefCell;

use crate::envelope::result::{has_result_shape, ResultBody};
use crate::envelope::{make_envelope, Envelope, EnvelopeId, MakeEnvelope};
use crate::errors::{BackpressureError, BoxError, BusError};
use crate::job::effect::{Effect, EffectHost};
use crate::job::job::{Control, Intent, Lifecycle, SharedIdGen, State};
use crate::job::job_lease::JobLease;
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
    pub hold: Option<JobLease>,
}

pub struct Context {
    buffer: RefCell<Buffer>,
    effects: RefCell<Vec<Effect>>,
    host: RefCell<Option<Rc<dyn EffectHost>>>,
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
                hold: None,
            }),
            effects: RefCell::new(Vec::new()),
            host: RefCell::new(None),
        }
    }

    pub fn set_host(&self, host: Rc<dyn EffectHost>) {
        *self.host.borrow_mut() = Some(host);
    }

    pub(crate) fn effect_host(&self) -> Result<Rc<dyn EffectHost>, BoxError> {
        self.host
            .borrow()
            .clone()
            .ok_or_else(|| Box::from(BusError("Effect host is not attached".into())))
    }

    pub fn tell(&self, topic: &str, payload: Payload) -> Result<(), BoxError> {
        self.effects.borrow_mut().push(Effect::Tell {
            topic: topic.into(),
            payload,
        });
        Ok(())
    }

    pub fn record(&self, topic: &str, payload: Payload) -> Result<(), BoxError> {
        self.effects.borrow_mut().push(Effect::Record {
            topic: topic.into(),
            payload,
        });
        Ok(())
    }

    pub fn barrier(&self, up_to: Option<u64>) -> Result<(), BoxError> {
        self.effects.borrow_mut().push(Effect::Barrier { up_to });
        Ok(())
    }

    pub fn timer(&self, deadline_ms: u64) -> Result<(), BoxError> {
        self.effects
            .borrow_mut()
            .push(Effect::Timer { deadline_ms });
        Ok(())
    }

    pub fn close(&self, reason: Option<String>) -> Result<(), BoxError> {
        self.effects.borrow_mut().push(Effect::Close { reason });
        Ok(())
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
        let asks = self
            .effects
            .borrow()
            .iter()
            .filter(|effect| matches!(effect, Effect::Ask { .. }))
            .count();
        let outcome = build_intent(&mut buffer, stage, topic, payload, timeout, asks);
        buffer.busy = false;
        let intent = outcome?;
        let request_id = intent.request.header.id.clone();
        drop(buffer);
        self.effects.borrow_mut().push(Effect::Ask { intent });
        Ok(request_id)
    }

    pub fn ask(
        &self,
        stage: &str,
        topic: &str,
        payload: Payload,
        timeout: Option<u64>,
    ) -> Result<Effect, BoxError> {
        let mut buffer = self.buffer.borrow_mut();
        if buffer.busy {
            return Err(Box::from(BusError("reentrant Context mutation".into())));
        }
        if buffer.id_gen.is_none() {
            return Err(Box::from(BusError(
                "Context is valid only during its callback".into(),
            )));
        }
        buffer.busy = true;
        let asks = self
            .effects
            .borrow()
            .iter()
            .filter(|effect| matches!(effect, Effect::Ask { .. }))
            .count();
        let outcome = build_intent(&mut buffer, stage, topic, payload, timeout, asks);
        buffer.busy = false;
        Ok(Effect::Ask { intent: outcome? })
    }

    // Pure builders: construct an Effect without pushing it, so a handler can
    // return a declarative effect list (mirrors the ts `*Effect` builders).
    // Combine with `Effect::apply` / `apply_effects` if you do not return them.

    pub fn reply_effect(&self, body: ResultBody) -> Effect {
        Effect::Reply(body)
    }

    pub fn finish_effect(&self, body: Option<ResultBody>) -> Effect {
        Effect::Finish(body)
    }

    pub fn hold_effect(
        &self,
        deadline_ms: Option<u64>,
        budget_ms: Option<u64>,
    ) -> Result<Effect, BoxError> {
        if !self.effects.borrow().is_empty() {
            return Err(Box::from(BusError(
                "cannot hold after other effects".into(),
            )));
        }
        self.edit(|buffer| {
            if buffer.hold.is_some() {
                return Err(Box::from(BusError("job already held".into())));
            }
            if buffer.control.is_some() || buffer.job.borrow().lifecycle != Lifecycle::Active {
                return Err(Box::from(BusError("job already finished".into())));
            }
            let root = buffer.job.borrow().root.clone();
            if root.header.address.kind() != "request" {
                return Err(Box::from(BusError("root is not a request".into())));
            }
            if buffer.reply.is_some() || buffer.job.borrow().replied {
                return Err(Box::from(BusError("root already replied".into())));
            }
            if !buffer.requests.is_empty() || buffer.pending_count > 0 {
                return Err(Box::from(BusError(
                    "cannot hold with pending requests".into(),
                )));
            }
            Ok(Effect::Hold(JobLease::with_options(deadline_ms, budget_ms)))
        })
    }

    pub fn settle_effect(&self, lease: JobLease, body: ResultBody) -> Effect {
        Effect::Settle(lease, body)
    }

    pub fn revoke_effect(&self, lease: JobLease, error: alloc::string::String) -> Effect {
        Effect::Revoke(lease, error)
    }

    pub fn tell_effect(&self, topic: &str, payload: Payload) -> Effect {
        Effect::Tell {
            topic: topic.into(),
            payload,
        }
    }

    pub fn record_effect(&self, topic: &str, payload: Payload) -> Effect {
        Effect::Record {
            topic: topic.into(),
            payload,
        }
    }

    pub fn barrier_effect(&self, up_to: Option<u64>) -> Effect {
        Effect::Barrier { up_to }
    }

    pub fn timer_effect(&self, deadline_ms: u64) -> Effect {
        Effect::Timer { deadline_ms }
    }

    pub fn close_effect(&self, reason: Option<alloc::string::String>) -> Effect {
        Effect::Close { reason }
    }

    pub(crate) fn push_intent(&self, intent: Intent) -> Result<(), BoxError> {
        let mut buffer = self.buffer.borrow_mut();
        if buffer.id_gen.is_none() {
            return Err(Box::from(BusError(
                "Context is valid only during its callback".into(),
            )));
        }
        buffer.requests.push(intent);
        Ok(())
    }

    pub fn settle(&self, lease: JobLease, body: ResultBody) -> Result<(), BoxError> {
        self.effects.borrow_mut().push(Effect::Settle(lease, body));
        Ok(())
    }

    pub fn revoke(&self, lease: JobLease, error: alloc::string::String) -> Result<(), BoxError> {
        self.effects.borrow_mut().push(Effect::Revoke(lease, error));
        Ok(())
    }

    pub fn reply(&self, body: ResultBody) -> Result<(), BoxError> {
        self.effects.borrow_mut().push(Effect::Reply(body));
        Ok(())
    }

    pub fn finish(&self, body: Option<ResultBody>) -> Result<(), BoxError> {
        self.effects.borrow_mut().push(Effect::Finish(body));
        Ok(())
    }

    pub fn hold(
        &self,
        deadline_ms: Option<u64>,
        budget_ms: Option<u64>,
    ) -> Result<JobLease, BoxError> {
        if !self.effects.borrow().is_empty() {
            return Err(Box::from(BusError(
                "cannot hold after other effects".into(),
            )));
        }
        self.edit(|buffer| {
            if buffer.hold.is_some() {
                return Err(Box::from(BusError("job already held".into())));
            }
            if buffer.control.is_some() || buffer.job.borrow().lifecycle != Lifecycle::Active {
                return Err(Box::from(BusError("job already finished".into())));
            }
            let root = buffer.job.borrow().root.clone();
            if root.header.address.kind() != "request" {
                return Err(Box::from(BusError("root is not a request".into())));
            }
            if buffer.reply.is_some() || buffer.job.borrow().replied {
                return Err(Box::from(BusError("root already replied".into())));
            }
            if !buffer.requests.is_empty() || buffer.pending_count > 0 {
                return Err(Box::from(BusError(
                    "cannot hold with pending requests".into(),
                )));
            }
            let lease = JobLease::with_options(deadline_ms, budget_ms);
            self.effects.borrow_mut().push(Effect::Hold(lease.clone()));
            Ok(lease)
        })
    }

    pub(crate) fn apply_reply(&self, body: ResultBody) -> Result<(), BoxError> {
        self.mutate(|buffer| put_reply(buffer, body))
    }

    pub(crate) fn apply_finish(&self, body: Option<ResultBody>) -> Result<(), BoxError> {
        self.mutate(|buffer| finish_control(buffer, body))
    }

    pub(crate) fn apply_hold(&self, lease: JobLease) -> Result<(), BoxError> {
        self.mutate(|buffer| {
            if buffer.hold.is_some() {
                return Err(Box::from(BusError("job already held".into())));
            }
            buffer.hold = Some(lease.clone());
            let mut state = buffer.job.borrow_mut();
            if state.lifecycle != Lifecycle::Active {
                return Err(Box::from(BusError("job already finished".into())));
            }
            state.held = true;
            state.reply_slot = Some(lease);
            Ok(())
        })
    }

    pub fn finished(&self) -> bool {
        let buffer = self.buffer.borrow();
        self.effects
            .borrow()
            .iter()
            .any(|effect| matches!(effect, Effect::Finish(_)))
            || buffer.control.is_some()
            || buffer.job.borrow().lifecycle == Lifecycle::Finished
    }

    pub fn effects(&self) -> Vec<Effect> {
        self.effects.borrow().clone()
    }

    fn mutate<F>(&self, action: F) -> Result<(), BoxError>
    where
        F: FnOnce(&mut Buffer) -> Result<(), BoxError>,
    {
        self.edit(action)
    }

    fn edit<T>(
        &self,
        action: impl FnOnce(&mut Buffer) -> Result<T, BoxError>,
    ) -> Result<T, BoxError> {
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

fn build_intent(
    buffer: &mut Buffer,
    stage: &str,
    topic: &str,
    payload: Payload,
    timeout: Option<u64>,
    extra: usize,
) -> Result<Intent, BoxError> {
    if buffer.hold.is_some() {
        return Err(Box::from(BusError("job already held".into())));
    }
    {
        let job = buffer.job.borrow();
        if buffer.pending_count + buffer.requests.len() + extra >= job.max_inflight {
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
        Ok(Intent {
            stage: stage.into(),
            request: Rc::new(request),
            timeout,
        })
    }
}

fn put_reply(buffer: &mut Buffer, body: ResultBody) -> Result<(), BoxError> {
    if buffer.hold.is_some() {
        return Err(Box::from(BusError("job already held".into())));
    }
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
    if buffer.hold.is_some() {
        return Err(Box::from(BusError("job already held".into())));
    }
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
