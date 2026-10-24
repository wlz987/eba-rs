use std::cell::RefCell;
use std::rc::Rc;

use crate::envelope::result::{err_payload, Completion};
use crate::envelope::{Envelope, EnvelopeId};
use crate::errors::BoxError;
use crate::job::call::{Call, Wait};
use crate::job::core::Core;
use crate::job::ending::drop_job;
use crate::job::host::JobHost;
use crate::job::job::Job;
use crate::mail::inbox::mark_read;

use super::{settle, Caller};

impl JobHost for Caller {
    fn core(&self) -> Rc<RefCell<Core>> {
        self.core.clone()
    }

    fn make_job(&self, root: Rc<Envelope>) -> Result<Job, BoxError> {
        Ok(Wait::new(root))
    }

    fn on_rejected(&self, root: Rc<Envelope>, reason: String) -> Result<(), BoxError> {
        let body = err_payload(&reason, None)?;
        settle(&root, &body, &root.header.id);
        Ok(())
    }
}

pub(super) fn end(_caller: &Caller, core: &mut Core, job: &Job) -> Result<bool, BoxError> {
    let root = job.root();
    let Some(call) = root.payload.downcast_ref::<Call>() else {
        return Ok(false);
    };
    let request_id = call.request_id.read().unwrap().clone();
    let Some(request_id) = request_id else {
        return Ok(false);
    };
    let body = err_payload("cancelled", None)?;
    core.book.cancel(&root.header.id)?;
    drop_job(core, job);
    mark_read(&core.sub.inbox);
    call.complete(Completion {
        request_id,
        body: Some(body),
    });
    Ok(true)
}

pub(super) fn mark(caller: &Caller, root_id: &EnvelopeId) -> bool {
    let Some(found) = caller.accepted_root(root_id) else {
        return false;
    };
    let call = found.payload.downcast_ref::<Call>();
    let Some(call) = call else {
        return false;
    };
    *call.cancelled.write().unwrap() = true;
    true
}
