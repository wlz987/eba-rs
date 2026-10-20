use crate::prelude::*;
use alloc::rc::Rc;
use alloc::sync::Arc;
use core::cell::RefCell;

use crate::envelope::result::err_payload;
use crate::envelope::{make_envelope, Envelope, EnvelopeId, MakeEnvelope};
use crate::errors::{BoxError, BusError, MailboxFull};
use crate::job::call::{Call, Wait};
use crate::job::core::Core;
use crate::job::host::JobHost;
use crate::job::job::Job;
use crate::mail::inbox::{enqueue, mark_read, reply_topic};

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

pub(super) fn end(caller: &Caller, core: &mut Core, job: &Job) -> Result<bool, BoxError> {
    let root = job.root();
    let Some(call) = root.payload.downcast_ref::<Call>() else {
        return Ok(false);
    };
    let request_id = call.request_id.read().clone();
    let Some(request_id) = request_id else {
        return Ok(false);
    };
    let inbox = core.sub.inbox.clone();
    if inbox.is_closed() {
        return Err(Box::from(BusError("inbox closed".into())));
    }
    if inbox.remaining() < 1 {
        mark_read(&inbox);
        return Err(Box::from(MailboxFull("target inbox full".into())));
    }
    let id = caller.id_gen.borrow_mut().next_envelope_id();
    let reply = make_envelope(MakeEnvelope {
        topic: reply_topic(&inbox),
        payload: Arc::new(err_payload("cancelled", None).unwrap()),
        from: core.sub.actor_id.clone(),
        cause: Some(job.root().header.cause.clone()),
        id: Some(id),
        kind: "result".into(),
        reply_to: None,
        correlation_id: Some(request_id),
    })
    .map_err(|error| Box::new(error) as BoxError)?;
    let rc = Rc::new(reply);
    if !enqueue(&inbox, rc) {
        mark_read(&inbox);
        return Err(Box::from(MailboxFull("target inbox full".into())));
    }
    mark_read(&inbox);
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
    *call.cancelled.write() = true;
    true
}
