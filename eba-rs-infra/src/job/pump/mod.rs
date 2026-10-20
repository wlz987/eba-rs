use crate::prelude::*;
use alloc::rc::Rc;

use crate::envelope::Envelope;
use crate::errors::{BoxError, BusError, REENTRANT};
use crate::job::core::{Core, HostState};
use crate::job::execution::{
    adopt, note_delivery_success, raise_delivery_failure, refused, reject, release_result, resume,
    resume_due, retry_due, waits_for_lease,
};
use crate::job::job::SharedIdGen;
use crate::mail::bus::Bus;
use crate::mail::inbox::{holds_kind, pause_work};
use crate::notify::signal::{DELIVERY, READ, TIMER, WORK};

pub const PHASE_FLAG: [u32; 4] = [READ, TIMER, WORK, DELIVERY];
mod advance;
mod step;

pub use self::advance::{advance_host, has_work};
pub use self::step::step;

pub(crate) const SIGNAL_MASK: u32 = READ | TIMER | WORK | DELIVERY;

fn dispatch(
    core: &mut Core,
    bus: &Bus,
    id_gen: &SharedIdGen,
    envelope: &Rc<Envelope>,
) -> Result<(), BoxError> {
    if envelope.header.address.kind() == "diagnostic" {
        return Ok(());
    }
    let mut reason = refused(core);
    if reason.is_none() && !core.queue.push(Rc::clone(envelope)) {
        reason = Some("queue_full".into());
    }
    match reason {
        Some(reason) => reject(core, bus, id_gen, envelope, &reason),
        None => Ok(()),
    }
}

fn recv(core: &Core) -> Option<Rc<Envelope>> {
    if core.sub.inbox.is_closed() {
        return None;
    }
    core.sub.try_recv().ok().flatten()
}

pub fn holding(core: &Core) -> bool {
    core.pending.is_some() || !core.rest.is_empty()
}

pub fn scheduled(core: &Core) -> bool {
    if waits_for_lease(core) || retry_due(core) || core.book.unreleased() > 0 {
        return true;
    }
    if !holding(core) {
        return core.signal.has(SIGNAL_MASK);
    }
    if core.queue.full() {
        if holds_kind(&core.sub.inbox, "result") {
            return true;
        }
        return core.signal.has(TIMER | DELIVERY);
    }
    core.signal.has(READ | TIMER | DELIVERY)
}

pub fn recompute_pause(core: &Core) {
    pause_work(&core.sub.inbox, holding(core) && core.queue.full());
}

pub fn arm_closed_waits(core: &mut Core) {
    if core.state != HostState::Running && core.state != HostState::Draining {
        return;
    }
    if !core.sub.inbox.is_closed() {
        return;
    }
    core.book.elapse_open();
}

pub fn refresh(core: &mut Core, now_ms: u64) {
    let is_holding = holding(core);
    recompute_pause(core);
    arm_closed_waits(core);
    core.signal.set(WORK, !is_holding && core.queue.size() > 0);
    let deadline = core.book.next_deadline_ms();
    core.signal
        .set(TIMER, deadline.is_some_and(|found| found <= now_ms));
}

pub(crate) fn enter(core: &mut Core, closing: bool) -> Result<(), BoxError> {
    if !closing {
        if core.state == HostState::Closed {
            return Err(Box::from(BusError("host closed".into())));
        }
        if core.state == HostState::Closing {
            return Err(Box::from(BusError("host closing".into())));
        }
    }
    if core.busy {
        return Err(Box::from(BusError(REENTRANT.into())));
    }
    core.busy = true;
    Ok(())
}

pub fn idle(core: &mut Core) {
    core.busy = false;
}

pub fn attach(core: &mut Core, bus: &Bus) -> Result<(), BoxError> {
    enter(core, false)?;
    let outcome = core.sub.start(bus);
    idle(core);
    outcome?;
    refresh(core, 0);
    Ok(())
}
