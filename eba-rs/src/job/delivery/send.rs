use std::rc::Rc;

use crate::envelope::reply::Matchmaker;
use crate::envelope::result::ResultBody;
use crate::envelope::Envelope;
use crate::errors::{combine_failures, is_outbox_full, BoxError};
use crate::job::core::Core;
use crate::job::ending::{drop_job, seal};
use crate::job::job::{Control, Job, SharedIdGen};
use crate::mail::bus::Bus;

use super::{
    complete_held, holds_letter, next_sendable, note_delivery_success, park,
    raise_delivery_failure, Slot,
};

pub fn hold_or_send(
    core: &mut Core,
    bus: &Bus,
    job: Option<Job>,
    envelope: Rc<Envelope>,
) -> Result<bool, BoxError> {
    match core.outbox.send(bus, Rc::clone(&envelope)) {
        Ok(()) => Ok(true),
        Err(error) => {
            let full = is_outbox_full(&error);
            park(core, job, Rc::clone(&envelope));
            if full {
                return Ok(false);
            }
            Err(raise_delivery_failure(core, error))
        }
    }
}

pub fn release_result(core: &mut Core, bus: &Bus) -> Result<bool, BoxError> {
    let mut sent = false;
    loop {
        let Some((slot, envelope, job)) = next_sendable(core) else {
            return Ok(sent);
        };
        if !core.outbox.has_room() {
            return Ok(sent);
        }
        let pending_slot = matches!(slot, Slot::Pending);
        let index = match slot {
            Slot::Pending => {
                core.pending = None;
                core.pending_job = None;
                0
            }
            Slot::Rest(index) => {
                core.rest.remove(index);
                index
            }
        };
        let pending_cleared = pending_slot && core.pending.is_none();
        if let Err(error) = core.outbox.send(bus, Rc::clone(&envelope)) {
            let full = is_outbox_full(&error);
            if pending_cleared {
                core.pending = Some(Rc::clone(&envelope));
                core.pending_job = job.clone();
            } else {
                let at = index.min(core.rest.len());
                core.rest.insert(at, (Rc::clone(&envelope), job.clone()));
            }
            if full {
                return Ok(sent);
            }
            return Err(raise_delivery_failure(core, error));
        }
        if !core.outbox.contains(&envelope) {
            note_delivery_success(core, &envelope);
        }
        complete_held(core, job)?;
        sent = true;
    }
}

pub(crate) fn defer_ending(
    core: &mut Core,
    bus: &Bus,
    job: &Job,
    control: Control,
    reply: Option<Rc<Envelope>>,
    error: BoxError,
) -> BoxError {
    let combined = error;
    job.state.borrow_mut().pending_control = Some(control);
    let Some(reply) = reply else {
        return combined;
    };
    if let Err(delivery_error) = hold_or_send(core, bus, Some(job.clone()), Rc::clone(&reply)) {
        if holds_letter(core, &reply) {
            job.state.borrow_mut().replied = true;
        }
        return combine_failures(combined, delivery_error);
    }
    job.state.borrow_mut().replied = true;
    combined
}

pub(crate) fn remove(core: &mut Core, job: &Job) -> Result<(), BoxError> {
    seal(core, job)?;
    drop_job(core, job);
    Ok(())
}

pub(crate) fn match_reply(
    core: &Core,
    id_gen: &SharedIdGen,
    root: &Envelope,
    body: &ResultBody,
) -> Result<Envelope, BoxError> {
    let maker = Matchmaker::new(&root.header)?;
    let id = id_gen.borrow_mut().next_envelope_id();
    Ok(maker.reply(body, core.sub.actor_id.clone(), id)?)
}
