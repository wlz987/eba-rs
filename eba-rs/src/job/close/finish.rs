use std::rc::Rc;

use crate::errors::{combine_failures, is_lease_pending, BoxError};
use crate::job::core::Core;
use crate::job::ending::release_ending;
use crate::job::job::{Job, Notice};

pub(super) fn finish_jobs(core: &mut Core) -> (Option<BoxError>, Option<BoxError>) {
    let jobs: Vec<Job> = core.jobs.values().cloned().collect();
    let mut hook_error: Option<BoxError> = None;
    for job in jobs {
        if job.state.borrow().pending_control.is_none() && job.state.borrow().notice == Notice::Open
        {
            let on_rejected = Rc::clone(&core.on_rejected);
            if let Err(error) = on_rejected(job.root(), "host_draining".into()) {
                hook_error = Some(match hook_error {
                    Some(held) => combine_failures(held, error),
                    None => error,
                });
            }
            job.state.borrow_mut().notice = Notice::Rejected;
        }
        let root = job.root();
        match release_ending(core, &job) {
            Ok(_) => core.abandoned_mut().push(root),
            Err(error) if is_lease_pending(&error) => return (hook_error, Some(error)),
            Err(error) => {
                let held = core.jobs.get(&root.header.id).cloned();
                if !held.is_some_and(|held| Rc::ptr_eq(&held.state, &job.state)) {
                    core.abandoned_mut().push(root);
                }
                hook_error = Some(match hook_error {
                    Some(held) => combine_failures(held, error),
                    None => error,
                });
            }
        }
    }
    (hook_error, None)
}
