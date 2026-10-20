use crate::prelude::*;
use alloc::rc::Rc;

use crate::envelope::Envelope;
use crate::errors::{is_lease_pending, note_settled, BoxError, LeasePending};
use crate::job::core::Core;
use crate::job::job::{set_lifecycle, Job, Lifecycle, Notice, State};

use super::{drop_job, notify};

pub fn release_ending(core: &mut Core, job: &Job) -> Result<Rc<Envelope>, BoxError> {
    let root = job.root();
    let ending = job.state.borrow_mut().pending_control.take();
    if let Err(error) = core.book.cancel(&root.header.id) {
        job.state.borrow_mut().pending_control = ending;
        return Err(if is_lease_pending(&error) {
            error
        } else {
            Box::from(LeasePending::caused(error))
        });
    }
    {
        let mut state = job.state.borrow_mut();
        state.pending_control = None;
        set_lifecycle(&mut state, Lifecycle::Finished);
    }
    let outcome = match ending {
        Some(ending) => notify(job, ending),
        None => Ok(()),
    };
    drop_job(core, job);
    let outcome = match outcome {
        Err(error) if matches!(job.state.borrow().notice, Notice::Finished) => {
            Err(note_settled(error))
        }
        other => other,
    };
    outcome?;
    Ok(root)
}

pub fn release_deferred(core: &mut Core) -> Result<bool, BoxError> {
    let mut released = false;
    let jobs: Vec<Job> = core.jobs.values().cloned().collect();
    for job in jobs {
        if !waits_for_book(core, &job.state.borrow()) {
            continue;
        }
        if let Err(error) = release_ending(core, &job) {
            if is_lease_pending(&error) {
                continue;
            }
            return Err(error);
        }
        released = true;
    }
    Ok(released)
}

pub fn waits_for_book(core: &Core, state: &State) -> bool {
    state.pending_control.is_some() && core.book.count(&state.root.header.id) > 0
}

pub fn waits_for_lease(core: &Core) -> bool {
    if core.book.size() == 0 {
        return false;
    }
    core.jobs
        .values()
        .any(|job| waits_for_book(core, &job.state.borrow()))
}
