use std::rc::Rc;
use std::sync::RwLock;

use crate::envelope::result::{err_payload, Completion, ResultBody};
use crate::envelope::{Envelope, EnvelopeId};
use crate::errors::{BoxError, BusError};
use crate::job::job::{Context, Job, JobHooks};
use crate::Payload;

const STAGE: &str = "reply";

pub struct Call {
    pub topic: String,
    pub payload: Payload,
    pub timeout_ms: Option<u64>,
    pub request_id: RwLock<Option<EnvelopeId>>,
    pub cancelled: RwLock<bool>,
    pub completion: RwLock<Option<Completion>>,
    pub pay: RwLock<Option<Pay>>,
}

pub enum Outcome {
    Settled(Completion),
    Failed(String),
}

pub type Pay = Box<dyn FnOnce(Outcome)>;

impl Call {
    pub fn new(topic: String, payload: Payload, timeout_ms: Option<u64>) -> Self {
        Call {
            topic,
            payload,
            timeout_ms,
            request_id: RwLock::new(None),
            cancelled: RwLock::new(false),
            completion: RwLock::new(None),
            pay: RwLock::new(None),
        }
    }

    pub fn set_pay(&self, pay: Pay) {
        *self.pay.write().unwrap() = Some(pay);
    }

    pub fn complete(&self, completion: Completion) {
        {
            let mut held = self.completion.write().unwrap();
            if held.is_some() {
                return;
            }
            *held = Some(completion.clone());
        }
        let pay = self.pay.write().unwrap().take();
        if let Some(pay) = pay {
            pay(Outcome::Settled(completion));
        }
    }
}

pub fn settle(root: &Rc<Envelope>, body: &ResultBody, request_id: &EnvelopeId) {
    let Some(call) = root.payload.downcast_ref::<Call>() else {
        return;
    };
    let chosen = call
        .request_id
        .read()
        .unwrap()
        .clone()
        .unwrap_or_else(|| request_id.clone());
    call.complete(Completion {
        request_id: chosen,
        body: Some(body.clone()),
    });
}

pub fn settled(envelope: &Rc<Envelope>) -> Option<Completion> {
    let call = envelope.payload.downcast_ref::<Call>()?;
    call.completion.read().unwrap().clone()
}

pub(crate) struct Wait {
    root: Rc<Envelope>,
}

impl Wait {
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(root: Rc<Envelope>) -> Job {
        Job::new(root.clone(), Rc::new(Wait { root }))
    }
}

impl JobHooks for Wait {
    fn begin(&self, ctx: &Context) -> Result<(), BoxError> {
        let root = ctx.root();
        let call = root
            .payload
            .downcast_ref::<Call>()
            .ok_or_else(|| Box::from(BusError("admitted root has no call".into())) as BoxError)?;
        if *call.cancelled.read().unwrap() {
            let completion = Completion {
                request_id: root.header.id.clone(),
                body: Some(err_payload("cancelled", None)?),
            };
            return <Wait as JobHooks>::on_stage_result(self, ctx, STAGE, Rc::new(completion));
        }
        let request_id = ctx.request(STAGE, &call.topic, call.payload.clone(), call.timeout_ms)?;
        *call.request_id.write().unwrap() = Some(request_id);
        Ok(())
    }

    fn on_stage_result(
        &self,
        ctx: &Context,
        _stage: &str,
        completion: Rc<Completion>,
    ) -> Result<(), BoxError> {
        let root = ctx.root();
        if let Some(call) = root.payload.downcast_ref::<Call>() {
            call.complete((*completion).clone());
        }
        Ok(())
    }

    fn on_finished(&self, body: Option<&ResultBody>) -> Result<(), BoxError> {
        if let Some(body) = body {
            settle(&self.root, body, &self.root.header.id);
        }
        Ok(())
    }
}
