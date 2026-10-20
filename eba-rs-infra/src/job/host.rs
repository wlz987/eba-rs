use crate::prelude::*;
use alloc::rc::Rc;
use core::cell::RefCell;

use crate::envelope::ids::ActorId;
use crate::envelope::{Envelope, EnvelopeId};
use crate::errors::BoxError;
use crate::job::close::close_host;
use crate::job::core::{Core, HostState};
use crate::job::execution::resume;
use crate::job::job::{Job, SharedIdGen};
use crate::job::pump::has_work;
use crate::mail::bus::Bus;
use crate::mail::inbox::{find, Inbox};
use crate::mail::queue::Queue;
use crate::time::clock::Clock;

fn queued(queue: &Queue<Rc<Envelope>>, envelope_id: &EnvelopeId) -> Option<Rc<Envelope>> {
    queue
        .iter()
        .find(|item| item.header.id == *envelope_id)
        .cloned()
}

pub trait JobHost {
    fn core(&self) -> Rc<RefCell<Core>>;
    fn make_job(&self, root: Rc<Envelope>) -> Result<Job, BoxError>;
    fn on_rejected(&self, root: Rc<Envelope>, reason: String) -> Result<(), BoxError>;

    fn actor_id(&self) -> ActorId {
        self.core().borrow().sub.actor_id.clone()
    }

    fn inbox(&self) -> Rc<Inbox> {
        self.core().borrow().sub.inbox.clone()
    }

    fn state(&self) -> HostState {
        self.core().borrow().state
    }

    fn admitted(&self) -> bool {
        self.core().borrow().signal.is_bound()
    }

    fn job(&self, request_id: &EnvelopeId) -> Option<Job> {
        self.core().borrow().jobs.get(request_id).cloned()
    }

    fn active_count(&self) -> usize {
        self.core().borrow().jobs.len()
    }

    fn queued_count(&self) -> usize {
        self.core().borrow().queue.size()
    }

    fn pending_count(&self) -> usize {
        self.core().borrow().book.size()
    }

    fn unsent_count(&self) -> usize {
        self.core().borrow().outbox.size()
    }

    fn rejected_count(&self) -> usize {
        self.core().borrow().rejected_count
    }

    fn next_deadline_ms(&self) -> Option<u64> {
        self.core().borrow().book.next_deadline_ms()
    }

    fn on_step(&self, _clock: &dyn Clock) {}

    fn crowded(&self) -> bool {
        let core_ref = self.core();
        let core = core_ref.borrow();
        let inbox = &core.sub.inbox;
        let sitting = if inbox.is_closed() {
            0
        } else {
            inbox.capacity - inbox.remaining()
        };
        core.jobs.len() + core.queue.size() + sitting >= core.active_limit
    }

    fn is_quiescent(&self) -> bool {
        let core = self.core();
        let core = core.borrow();
        !core.busy && !has_work(&core)
    }

    fn accepted_root(&self, root_id: &EnvelopeId) -> Option<Rc<Envelope>> {
        let core_ref = self.core();
        let core = core_ref.borrow();
        find(&core.sub.inbox, root_id).or_else(|| queued(&core.queue, root_id))
    }

    fn delivery_spent(&self, envelope: &Rc<Envelope>) -> bool {
        let core_ref = self.core();
        let core = core_ref.borrow();
        let key = &envelope.header.id;
        core.attempts.contains_key(key) && !core.again.contains(key)
    }

    fn resume_held(
        &self,
        request_id: &EnvelopeId,
        failure: &Rc<Envelope>,
        bus: &Bus,
        clock: &dyn Clock,
        id_gen: &SharedIdGen,
        after: &dyn Fn(),
    ) -> Result<bool, BoxError> {
        let core_ref = self.core();
        if !core_ref.borrow().book.holds(request_id) {
            return Ok(false);
        }
        {
            let core = core_ref.borrow();
            if core.busy || (core.state != HostState::Running && core.state != HostState::Draining)
            {
                return Ok(true);
            }
        }
        let outcome = resume(&mut core_ref.borrow_mut(), bus, clock, id_gen, failure);
        after();
        outcome?;
        Ok(true)
    }

    fn close(&self, bus: &Bus) -> Result<Vec<Rc<Envelope>>, BoxError> {
        let core_ref = self.core();
        if core_ref.borrow().signal.is_bound() {
            return Err(Box::from(crate::errors::BusError(
                "runtime owns host; use Runtime.close".into(),
            )));
        }
        let abandoned = {
            let mut core = core_ref.borrow_mut();
            let abandoned = close_host(&mut core, bus)?;
            core.abandoned = None;
            abandoned
        };
        Ok(abandoned)
    }
}

pub fn core_of(host: &dyn JobHost) -> Rc<RefCell<Core>> {
    host.core()
}
