use alloc::rc::Rc;

use crate::envelope::Envelope;
use crate::errors::{combine_failures, BoxError};
use crate::job::core::Core;
use crate::job::ending::{settle_ending, waits_for_book};
use crate::job::hook::delivery_envelope;
use crate::job::job::Job;

/// `tests/parking.rs` 的 `the_pending_place_is_marked_minus_one` 就按这个名字问它。
pub enum Slot {
    Pending,
    Rest(usize),
}

pub(super) fn park(core: &mut Core, job: Option<Job>, envelope: Rc<Envelope>) {
    if core.pending.is_none() {
        core.pending = Some(envelope);
        core.pending_job = job;
    } else {
        core.rest.push((envelope, job));
    }
}

pub fn holds_letter(core: &Core, envelope: &Rc<Envelope>) -> bool {
    let id = &envelope.header.id;
    if core
        .pending
        .as_ref()
        .is_some_and(|held| &held.header.id == id)
    {
        return true;
    }
    core.rest.iter().any(|(held, _)| &held.header.id == id)
}

pub(super) fn owns(core: &Core, envelope: &Rc<Envelope>) -> bool {
    holds_letter(core, envelope) || core.outbox.contains(envelope)
}

pub fn note_delivery_failure(core: &mut Core, envelope: &Rc<Envelope>) -> Result<(), BoxError> {
    if !owns(core, envelope) {
        return Ok(());
    }
    let key = envelope.header.id.clone();
    let count = core.attempts.get(&key).copied().unwrap_or(0) + 1;
    core.attempts.insert(key.clone(), count);
    if count < 2 {
        core.again.insert(key);
        core.outbox.rearm(envelope);
        return Ok(());
    }
    let job = parked_job(core, envelope);
    core.again.remove(&key);
    unpark(core, envelope);
    core.outbox.bury(envelope);
    complete_held(core, job)
}

pub fn release_attempts(core: &mut Core) {
    core.attempts.clear();
    core.again.clear();
}

pub fn raise_delivery_failure(core: &mut Core, error: BoxError) -> BoxError {
    let Some(envelope) = delivery_envelope(&error) else {
        return error;
    };
    match note_delivery_failure(core, &envelope) {
        Ok(()) => error,
        Err(secondary) => combine_failures(error, secondary),
    }
}

pub(super) fn parked_job(core: &Core, envelope: &Rc<Envelope>) -> Option<Job> {
    if core
        .pending
        .as_ref()
        .is_some_and(|held| held.header.id == envelope.header.id)
    {
        return core.pending_job.clone();
    }
    core.rest
        .iter()
        .find(|(held, _)| held.header.id == envelope.header.id)
        .and_then(|(_, job)| job.clone())
}

pub(super) fn unpark(core: &mut Core, envelope: &Rc<Envelope>) {
    if core
        .pending
        .as_ref()
        .is_some_and(|held| held.header.id == envelope.header.id)
    {
        core.pending = None;
        core.pending_job = None;
    }
    core.rest
        .retain(|(held, _)| held.header.id != envelope.header.id);
}

pub fn note_delivery_success(core: &mut Core, envelope: &Rc<Envelope>) {
    let key = envelope.header.id.clone();
    core.attempts.remove(&key);
    core.again.remove(&key);
}

pub(super) fn spent(core: &Core, envelope: &Rc<Envelope>) -> bool {
    let key = &envelope.header.id;
    core.attempts.contains_key(key) && !core.again.contains(key)
}

pub fn retry_due(core: &Core) -> bool {
    core.outbox.has_room() && next_sendable(core).is_some()
}

pub fn next_sendable(core: &Core) -> Option<(Slot, Rc<Envelope>, Option<Job>)> {
    if let Some(pending) = core.pending.as_ref() {
        if !spent(core, pending) {
            return Some((Slot::Pending, Rc::clone(pending), core.pending_job.clone()));
        }
    }
    for (index, (envelope, job)) in core.rest.iter().enumerate() {
        if !spent(core, envelope) {
            return Some((Slot::Rest(index), Rc::clone(envelope), job.clone()));
        }
    }
    None
}

pub(crate) fn complete_held(core: &mut Core, job: Option<Job>) -> Result<(), BoxError> {
    let Some(job) = job else {
        return Ok(());
    };
    if waits_for_book(core, &job.state.borrow()) {
        return Ok(());
    }
    let control = job.state.borrow_mut().pending_control.take();
    settle_ending(core, &job, control)
}
