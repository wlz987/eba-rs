use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::vec::Vec;

use super::execution::reject;
use super::hook::run_hook;
use crate::envelope::Envelope;
use crate::errors::{combine_failures, BoxError};
use crate::job::book::Continuation;
use crate::job::core::Core;
use crate::job::job::{set_lifecycle, Job, Lifecycle, SharedIdGen};
use crate::mail::bus::Bus;
use crate::time::clock::Clock;

pub fn adopt(
    core: &mut Core,
    bus: &Bus,
    clock: &dyn Clock,
    id_gen: &SharedIdGen,
    root: Rc<Envelope>,
) -> Result<(), BoxError> {
    let root_id = root.header.id.clone();
    if core.jobs.contains_key(&root_id) {
        return reject(core, bus, id_gen, &root, "adoption_failed");
    }
    let staged = (|| -> Result<Job, BoxError> {
        let job = (core.make_job)(Rc::clone(&root))?;
        {
            let state = job.state.borrow();
            if !Rc::ptr_eq(&state.root, &root) || state.lifecycle != Lifecycle::New {
                return Err(Box::from(crate::errors::BusError(
                    "factory must return a fresh Job for this root".into(),
                )));
            }
        }
        Ok(job)
    })();
    let job = match staged {
        Ok(job) => job,
        Err(error) => {
            return match reject(core, bus, id_gen, &root, "adoption_failed") {
                Ok(()) => Err(error),
                Err(secondary) => Err(combine_failures(error, secondary)),
            };
        }
    };
    set_lifecycle(&mut job.state.borrow_mut(), Lifecycle::Active);
    core.jobs.insert(root_id, job.clone());
    let hooks = Rc::clone(&job.hooks);
    run_hook(core, bus, clock, id_gen, &job, |ctx| hooks.begin(ctx))
}

fn continue_job(
    core: &mut Core,
    bus: &Bus,
    clock: &dyn Clock,
    id_gen: &SharedIdGen,
    claim: Option<crate::rendezvous::records::Claim<Continuation>>,
) -> Result<(), BoxError> {
    let Some(claim) = claim else {
        return Ok(());
    };
    let continuation = claim.continuation;
    let owner = continuation.owner.clone();
    let stage = continuation.stage.clone();
    let job = core.jobs.get(&owner).cloned();
    let Some(job) = job else {
        return Ok(());
    };
    let hooks = Rc::clone(&job.hooks);
    let completion = Rc::new(claim.completion);
    run_hook(core, bus, clock, id_gen, &job, |ctx| {
        hooks.on_stage_result(ctx, &stage, completion.clone())
    })
}

pub fn keep_unread(core: &mut Core, envelope: &Rc<Envelope>) {
    if core.abandoned.is_none() {
        core.abandoned = Some(Vec::new());
    }
    core.abandoned
        .as_mut()
        .expect("the handback list")
        .push(Rc::clone(envelope));
}

pub fn resume(
    core: &mut Core,
    bus: &Bus,
    clock: &dyn Clock,
    id_gen: &SharedIdGen,
    source: &Rc<Envelope>,
) -> Result<(), BoxError> {
    let claim = match core.book.take_result(source) {
        Ok(claim) => claim,
        Err(error) => {
            if error
                .downcast_ref::<crate::rendezvous::records::UnreadableResult>()
                .is_some()
            {
                keep_unread(core, source);
                return Ok(());
            }
            core.abandoned_mut().push(Rc::clone(source));
            return Err(error);
        }
    };
    continue_job(core, bus, clock, id_gen, claim)
}

pub fn resume_due(
    core: &mut Core,
    bus: &Bus,
    clock: &dyn Clock,
    id_gen: &SharedIdGen,
    now_ms: u64,
) -> Result<(), BoxError> {
    let claim = core.book.take_due(now_ms);
    continue_job(core, bus, clock, id_gen, claim)
}
