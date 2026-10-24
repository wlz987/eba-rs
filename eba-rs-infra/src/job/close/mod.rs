use alloc::boxed::Box;
use alloc::rc::Rc;

mod finish;

use self::finish::finish_jobs;
use alloc::vec::Vec;

use crate::envelope::Envelope;
use crate::errors::{combine_failures, BoxError, LeasePending};
use crate::job::core::{Core, HostState};
use crate::job::delivery::release_attempts;
use crate::job::held::close_held;
use crate::job::job::Notice;
use crate::job::pump::{enter, idle, recompute_pause, SIGNAL_MASK};
use crate::mail::bus::Bus;
use crate::mail::inbox::{held_count, take_seized};

fn settle_letter(
    core: &mut Core,
    envelope: &Rc<Envelope>,
    notice: usize,
) -> Result<usize, BoxError> {
    core.abandoned_mut().push(Rc::clone(envelope));
    if notice == 0 {
        return Ok(0);
    }
    let on_rejected = Rc::clone(&core.on_rejected);
    on_rejected(Rc::clone(envelope), "host_draining".into())?;
    Ok(notice - 1)
}

fn settle_inbound(core: &mut Core, mut notice: usize) -> Result<(), BoxError> {
    while let Some(item) = core.queue.shift() {
        notice = settle_letter(core, &item, notice.max(1))?;
    }
    if core.sub.inbox.is_closed() {
        return Ok(());
    }
    loop {
        let Some(envelope) = core.sub.try_recv()? else {
            return Ok(());
        };
        notice = settle_letter(core, &envelope, notice)?;
    }
}

pub fn close_host(core: &mut Core, bus: &Bus) -> Result<Vec<Rc<Envelope>>, BoxError> {
    if core.state == HostState::Closed {
        return Ok(core.abandoned.clone().unwrap_or_default());
    }
    enter(core, true)?;
    core.begin_close();
    let outcome = close_pass(core, bus);
    idle(core);
    outcome?;
    Ok(core.abandoned.clone().unwrap_or_default())
}

fn close_pass(core: &mut Core, bus: &Bus) -> Result<(), BoxError> {
    close_held(core);
    let (hook_error, lease_error) = finish_jobs(core);
    settle_inbound(core, core.queue.size() + held_count(&core.sub.inbox))?;
    recompute_pause(core);
    if let Some(error) = lease_error {
        return Err(match hook_error {
            Some(hook) => combine_failures(hook, error),
            None => error,
        });
    }
    core.book.recover();
    if core.book.unreleased() > 0 || core.book.size() > 0 {
        let pending: BoxError = Box::from(LeasePending::new());
        return Err(match hook_error {
            Some(hook) => combine_failures(hook, pending),
            None => pending,
        });
    }
    let drained = core.outbox.drain();
    core.abandoned_mut().extend(drained);

    while core.pending.is_some() || !core.rest.is_empty() {
        let (envelope, job) = if core.pending.is_none() {
            core.rest.remove(0)
        } else {
            let envelope = core.pending.take().expect("checked above");
            let job = core.pending_job.take();
            (envelope, job)
        };
        let finished = job
            .as_ref()
            .is_some_and(|job| job.state.borrow().notice == Notice::Finished);
        if let Some(job) = &job {
            job.state.borrow_mut().pending_control = None;
        }
        if finished {
            core.abandoned_mut().push(envelope);
        } else {
            settle_letter(core, &envelope, 1)?;
        }
    }
    core.sub.close(bus)?;
    let seized = take_seized(&core.sub.inbox);
    for envelope in seized {
        core.abandoned_mut().push(envelope);
    }
    core.signal.clear(SIGNAL_MASK);
    core.state = HostState::Closed;
    release_attempts(core);
    match hook_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}
