use crate::prelude::*;
use alloc::collections::BTreeMap;
use alloc::rc::Rc;
use alloc::sync::Arc;

use crate::envelope::result::err_payload;
use crate::errors::{combine_failures, BoxError};
use crate::job::core::Core;
use crate::job::delivery::{hold_or_send, holds_letter, match_reply, remove};
use crate::job::job::{set_lifecycle, Job, Lifecycle, Notice, SharedIdGen};
use crate::mail::bus::Bus;
use crate::Payload;

pub(super) fn fail_job(
    core: &mut Core,
    bus: &Bus,
    id_gen: &SharedIdGen,
    job: &Job,
    error: BoxError,
) -> BoxError {
    let mut error = error;
    set_lifecycle(&mut job.state.borrow_mut(), Lifecycle::Finished);
    let mut detail: BTreeMap<String, Payload> = BTreeMap::new();
    detail.insert("message".into(), Arc::new(error.to_string()));
    let failure = match err_payload("job_failed", Some(detail)) {
        Ok(body) => body,
        Err(build) => return combine_failures(error, Box::new(build)),
    };
    if let Err(secondary) = job.hooks.on_finished(Some(&failure)) {
        error = combine_failures(error, secondary);
    }
    job.state.borrow_mut().notice = Notice::Finished;
    if let Err(secondary) = remove(core, job) {
        core.begin_close();
        error = combine_failures(error, secondary);
    }
    let root = job.root();
    let replied = job.state.borrow().replied;
    if root.header.address.kind() == "request" && !replied {
        let built = match_reply(core, id_gen, &root, &failure);
        match built {
            Ok(built) => {
                let built = Rc::new(built);
                match hold_or_send(core, bus, None, Rc::clone(&built)) {
                    Ok(_) => job.state.borrow_mut().replied = true,
                    Err(delivery_error) => {
                        if holds_letter(core, &built) {
                            job.state.borrow_mut().replied = true;
                        }
                        error = combine_failures(error, delivery_error);
                    }
                }
            }
            Err(delivery_error) => error = combine_failures(error, delivery_error),
        }
    }
    error
}
