use crate::errors::{BoxError, DeliveryError};
use crate::job::core::Core;
use crate::job::job::SharedIdGen;
use crate::job::outbox::Step;
use crate::mail::bus::Bus;
use crate::mail::inbox::pull_kind;
use crate::notify::signal::DELIVERY;
use crate::time::clock::Clock;

use super::{
    adopt, dispatch, holding, note_delivery_success, raise_delivery_failure, recv, refresh,
    refused, reject, release_result, resume, resume_due, retry_due, PHASE_FLAG,
};

pub fn step(
    core: &mut Core,
    bus: &Bus,
    clock: &dyn Clock,
    id_gen: &SharedIdGen,
    phase: usize,
) -> Result<Step, BoxError> {
    if holding(core) && phase == 2 {
        return Ok(Step::Idle);
    }
    if holding(core) && phase == 0 {
        if let Some(envelope) = pull_kind(&core.sub.inbox, "result") {
            resume(core, bus, clock, id_gen, &envelope)?;
            return Ok(Step::Advanced);
        }
        if core.queue.full() {
            return Ok(Step::Idle);
        }
        let Some(received) = recv(core) else {
            return Ok(Step::Idle);
        };
        if received.header.address.kind() != "diagnostic" {
            core.queue.push(received);
        }
        return Ok(Step::Advanced);
    }
    if phase == 3 && (retry_due(core) || core.signal.has(DELIVERY)) {
        let mut step_value = match core.outbox.flush_one(bus) {
            Ok((flush_step, sent)) => {
                if let Some(sent) = sent {
                    note_delivery_success(core, &sent);
                }
                flush_step
            }
            Err(error) => {
                if error.downcast_ref::<DeliveryError>().is_some() {
                    return Err(raise_delivery_failure(core, error));
                }
                return Err(error);
            }
        };
        if release_result(core, bus)? && step_value == Step::Idle {
            step_value = Step::Advanced;
        }
        return Ok(step_value);
    }
    if !core.signal.has(PHASE_FLAG[phase]) {
        return Ok(Step::Idle);
    }
    if phase == 0 {
        let Some(envelope) = recv(core) else {
            return Ok(Step::Idle);
        };
        if envelope.header.address.kind() == "result" {
            resume(core, bus, clock, id_gen, &envelope)?;
        } else {
            dispatch(core, bus, id_gen, &envelope)?;
        }
        return Ok(Step::Advanced);
    }
    if phase == 1 {
        resume_due(core, bus, clock, id_gen, clock.now_ms())?;
        return Ok(Step::Advanced);
    }
    let Some(root) = core.queue.shift() else {
        return Ok(Step::Idle);
    };
    let mut reason = refused(core);
    if reason.is_none() && core.jobs.len() >= core.active_limit {
        reason = Some("active_full".into());
    }
    if let Some(reason) = reason {
        reject(core, bus, id_gen, &root, &reason)?;
    } else {
        adopt(core, bus, clock, id_gen, root)?;
    }
    Ok(Step::Advanced)
}

pub(super) fn pump(
    core: &mut Core,
    bus: &Bus,
    clock: &dyn Clock,
    id_gen: &SharedIdGen,
    mut budget: usize,
) -> Result<bool, BoxError> {
    let mut progressed = false;
    let mut idle = 0;
    while budget > 0 && idle < 4 {
        let phase = core.phase;
        core.phase = (phase + 1) % 4;
        let now = clock.now_ms();
        let advanced = step(core, bus, clock, id_gen, phase);
        refresh(core, now);
        let found = advanced?;
        if found == Step::Idle {
            idle += 1;
            continue;
        }
        budget -= 1;
        progressed = progressed || found == Step::Advanced;
        idle = 0;
    }
    Ok(progressed)
}
