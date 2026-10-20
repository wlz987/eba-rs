use std::rc::Rc;

use crate::errors::{note_settled, BoxError};
use crate::job::core::{Core, HostState};
use crate::job::job::{set_lifecycle, Control, Job, Lifecycle, Notice};

mod release;

pub use self::release::{release_deferred, release_ending, waits_for_book, waits_for_lease};

pub fn refused(core: &Core) -> Option<String> {
    (core.state != HostState::Running).then(|| "host_draining".into())
}

pub fn seal(core: &mut Core, job: &Job) -> Result<(), BoxError> {
    {
        let mut state = job.state.borrow_mut();
        if state.lifecycle == Lifecycle::Active {
            set_lifecycle(&mut state, Lifecycle::Delivering);
        }
    }
    let root_id = job.root().header.id.clone();
    core.book.cancel(&root_id)
}

pub(crate) fn drop_job(core: &mut Core, job: &Job) {
    let request_id = job.root().header.id.clone();
    let held = core.jobs.get(&request_id).cloned();
    if held.is_some_and(|held| same_job(&held, job)) {
        core.jobs.remove(&request_id);
    }
}

fn notify(job: &Job, control: Control) -> Result<(), BoxError> {
    {
        let mut state = job.state.borrow_mut();
        set_lifecycle(&mut state, Lifecycle::Finished);
        state.notice = Notice::Finished;
    }
    job.hooks.on_finished(control.body.as_ref())
}

fn same_job(a: &Job, b: &Job) -> bool {
    Rc::ptr_eq(&a.state, &b.state)
}

pub fn leave_ending(job: &Job, control: Option<Control>) {
    if let Some(control) = control {
        job.state.borrow_mut().pending_control = Some(control);
    }
}

pub(crate) fn settle_ending(
    core: &mut Core,
    job: &Job,
    control: Option<Control>,
) -> Result<(), BoxError> {
    let Some(control) = control else {
        return Ok(());
    };
    let outcome = notify(job, control);
    drop_job(core, job);
    match outcome {
        Err(error) if matches!(job.state.borrow().notice, Notice::Finished) => {
            Err(note_settled(error))
        }
        other => other,
    }
}
