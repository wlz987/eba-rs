use crate::prelude::*;
use alloc::rc::Rc;

use crate::envelope::result::{err_payload, ResultBody};
use crate::envelope::Envelope;
use crate::errors::{combine_failures, BoxError};
use crate::job::core::Core;
use crate::job::job::{Commit, Control, Job, SharedIdGen};
use crate::mail::bus::Bus;
use crate::mail::inbox::reply_topic;
use crate::time::clock::Clock;
use crate::time::numbers::nonnegative_int;

use super::delivery::match_reply;
pub use super::delivery::{
    hold_or_send, next_sendable, note_delivery_failure, note_delivery_success,
    raise_delivery_failure, release_attempts, release_result, retry_due, Slot,
};
pub use super::ending::{
    leave_ending, refused, release_deferred, release_ending, seal, waits_for_book, waits_for_lease,
};
pub use super::hook::deliver_ending;
pub use super::resume::{adopt, keep_unread, resume, resume_due};

fn respond(
    core: &mut Core,
    bus: &Bus,
    id_gen: &SharedIdGen,
    root: &Envelope,
    body: &ResultBody,
) -> Result<(), BoxError> {
    let reply = match_reply(core, id_gen, root, body)?;
    hold_or_send(core, bus, None, Rc::new(reply))?;
    Ok(())
}

pub fn reject(
    core: &mut Core,
    bus: &Bus,
    id_gen: &SharedIdGen,
    root: &Rc<Envelope>,
    reason: &str,
) -> Result<(), BoxError> {
    core.rejected_count += 1;
    let mut sending = Ok(());
    if root.header.address.kind() == "request" {
        let body = err_payload(reason, None)?;
        sending = respond(core, bus, id_gen, root, &body);
    }
    let observed = (core.on_rejected)(Rc::clone(root), reason.to_string());
    match (sending, observed) {
        (Ok(()), _) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(secondary)) => Err(combine_failures(error, secondary)),
    }
}

fn deadline(core: &Core, clock: &dyn Clock, timeout: Option<u64>) -> Option<u64> {
    let chosen = match timeout {
        Some(value) => value,
        None => core.timeout?,
    };
    let now = nonnegative_int(clock.now_ms(), "clock_ms");
    Some(nonnegative_int(now + chosen, "deadline_ms"))
}

/// 补答与否已经被 `commit` 改写过一次。py 的 `tests/test_lifecycle.py` 按名直接问这一句。
pub fn silence(
    core: &Core,
    job: &Job,
    id_gen: &SharedIdGen,
    control: Control,
) -> Result<(Option<Rc<Envelope>>, Control), BoxError> {
    let state = job.state.borrow();
    let root = Rc::clone(&state.root);
    let own_reply = reply_topic(&core.sub.inbox);
    let self_addressed = root.header.address.reply_to() == Some(own_reply.as_str());
    let held = core.book.holds(&root.header.id);
    if state.replied || root.header.address.kind() != "request" || (self_addressed && !held) {
        return Ok((None, control));
    }
    drop(state);
    let body = err_payload("job_silent", None)?;
    let envelope = match_reply(core, id_gen, &root, &body)?;
    Ok((Some(Rc::new(envelope)), Control { body: Some(body) }))
}

pub(crate) fn commit_to(
    core: &mut Core,
    bus: &Bus,
    clock: &dyn Clock,
    id_gen: &SharedIdGen,
    job: &Job,
    commit: Commit,
) -> Result<(Option<Rc<Envelope>>, Option<Control>), BoxError> {
    let root = job.root();
    let state = Rc::clone(&job.state);
    let mut reply: Option<Rc<Envelope>> = None;
    if let Some(body) = &commit.reply {
        let envelope = match_reply(core, id_gen, &root, body)?;
        reply = Some(Rc::new(envelope));
        state.borrow_mut().reply_body = Some(body.clone());
    }
    let mut control = commit.control;
    if control.is_none() {
        let inbox = Rc::clone(&core.sub.inbox);
        for intent in &commit.requests {
            core.book.submit(
                bus,
                &inbox,
                Rc::clone(&intent.request),
                root.header.id.clone(),
                intent.stage.clone(),
                deadline(core, clock, intent.timeout),
            )?;
        }
        if core.book.count(&root.header.id) == 0 {
            control = Some(Control {
                body: state.borrow().reply_body.clone(),
            });
        }
    } else if control.as_ref().is_some_and(|held| held.body.is_none()) {
        control = Some(Control {
            body: state.borrow().reply_body.clone(),
        });
    }
    Ok((reply, control))
}
