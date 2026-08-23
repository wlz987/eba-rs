pub(crate) mod quad;
pub(crate) mod topic;

pub use topic::result_topic_of;

use crate::bus::Bus;
use crate::envelope::{make_envelope_with, ActorId, Envelope, EnvelopeId};
use crate::idgen::IdGenHandle;
use crate::inbox::InboxHandle;
use crate::registry::quad::{quad_ok, Entry};
use crate::result::body::result_request_id;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Pending,
    Resolved,
    TimedOut,
    Failed,
}

#[derive(Debug, Clone, Default)]
pub struct ResolveOutcome {
    pub state: Option<State>,
    pub request_id: Option<EnvelopeId>,
    pub fresh: bool,
}

#[derive(Debug, Clone)]
pub struct StartParams {
    pub request_prefix: String,
    pub result_prefix: String,
    pub payload: crate::envelope::Value,
    pub from: ActorId,
    pub cause: EnvelopeId,
    pub request_id: Option<EnvelopeId>,
}

#[derive(Debug, Default)]
pub struct Registry {
    entries: HashMap<EnvelopeId, Entry>,
    id_gen: Option<IdGenHandle>,
}

impl Registry {
    pub fn new() -> Registry {
        Registry::default()
    }

    pub(crate) fn bind_id_gen(&mut self, gen: &IdGenHandle) {
        self.id_gen = Some(gen.clone());
    }

    fn require_gen(&self) -> crate::Result<IdGenHandle> {
        self.id_gen.clone().ok_or_else(|| {
            crate::Error::State("IdGen bound by start_request or JobHost.handle".into())
        })
    }

    pub fn start_request(
        &mut self,
        bus: &Bus,
        inbox: &InboxHandle,
        gen: &IdGenHandle,
        params: StartParams,
    ) -> crate::Result<Envelope> {
        self.bind_id_gen(gen);
        let eid = match params.request_id {
            Some(id) => id,
            None => gen.borrow_mut().next_envelope_id(),
        };
        let seg = gen.borrow().topic_segment(&eid)?;
        let request = make_envelope_with(
            &format!("{}.{seg}", params.request_prefix),
            params.payload,
            params.from.clone(),
            Some(params.cause.clone()),
            Some(eid.clone()),
            gen,
        )?;
        let result_topic = format!("{}.{seg}", params.result_prefix);
        self.expect(&request, &result_topic, gen)?;
        if let Err(err) = bus.subscribe(&result_topic, inbox) {
            self.drop_entry(&eid);
            return Err(err);
        }
        if let Err(err) = bus.publish(&request) {
            self.set_terminal(&eid, State::Failed);
            self.finish_safe(bus, inbox, &eid);
            return Err(err);
        }
        Ok(request)
    }

    pub fn resolve_only(&mut self, env: &Envelope) -> crate::Result<ResolveOutcome> {
        let gen = self.require_gen()?;
        let request_id = result_request_id(env);
        let prior = request_id.as_ref().and_then(|id| self.state(id));
        let state = self.apply_quad(env, &gen);
        let fresh = prior == Some(State::Pending) && state == Some(State::Resolved);
        Ok(ResolveOutcome {
            state,
            request_id,
            fresh,
        })
    }

    pub fn finish_safe(&mut self, bus: &Bus, inbox: &InboxHandle, request_id: &EnvelopeId) {
        if let Some(entry) = self.entries.get(request_id) {
            let topic = entry.expected_topic.clone();
            let _ = bus.unsubscribe(&topic, inbox);
        }
        self.entries.remove(request_id);
    }

    pub fn state(&self, request_id: &EnvelopeId) -> Option<State> {
        self.entries.get(request_id).map(|e| e.state)
    }

    pub(crate) fn timeout(&mut self, request_id: &EnvelopeId) {
        self.set_terminal(request_id, State::TimedOut);
    }

    fn expect(
        &mut self,
        request: &Envelope,
        expected_topic: &str,
        gen: &IdGenHandle,
    ) -> crate::Result<()> {
        let request_id = request.header.id.clone();
        let segment = gen.borrow().topic_segment(&request_id)?;
        let suffix = crate::envelope::topic_suffix(expected_topic).map_err(crate::Error::from)?;
        if suffix != segment {
            return Err(crate::Error::State(format!(
                "expected_topic {expected_topic:?} does not end with topic_segment(request_id) {segment:?}"
            )));
        }
        self.entries.remove(&request_id);
        self.entries.insert(
            request_id,
            Entry {
                expected_topic: expected_topic.into(),
                cause: request.header.cause.clone(),
                state: State::Pending,
            },
        );
        Ok(())
    }

    fn apply_quad(&mut self, env: &Envelope, gen: &IdGenHandle) -> Option<State> {
        let request_id = result_request_id(env)?;
        let entry = self.entries.get(&request_id)?;
        if !quad_ok(&request_id, entry, env, gen) {
            return None;
        }
        if entry.state != State::Pending {
            return Some(entry.state);
        }
        self.entries.get_mut(&request_id)?.state = State::Resolved;
        Some(State::Resolved)
    }

    fn drop_entry(&mut self, request_id: &EnvelopeId) {
        self.entries.remove(request_id);
    }

    fn set_terminal(&mut self, request_id: &EnvelopeId, state: State) {
        if let Some(entry) = self.entries.get_mut(request_id) {
            if entry.state == State::Pending {
                entry.state = state;
            }
        }
    }
}
