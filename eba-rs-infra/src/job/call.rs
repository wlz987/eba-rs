use crate::prelude::*;
use alloc::rc::Rc;
#[cfg(feature = "std")]
use core::cell::{Cell, RefCell};
use spin::RwLock;

use crate::envelope::result::{err_payload, Completion, ResultBody};
use crate::envelope::{Envelope, EnvelopeId};
use crate::errors::{BoxError, BusError};
use crate::job::effect::Effect;
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
    #[cfg(feature = "std")]
    pay_errors: Cell<usize>,
    #[cfg(feature = "std")]
    last_pay_error: RefCell<Option<String>>,
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
            #[cfg(feature = "std")]
            pay_errors: Cell::new(0),
            #[cfg(feature = "std")]
            last_pay_error: RefCell::new(None),
        }
    }

    pub fn set_pay(&self, pay: Pay) {
        *self.pay.write() = Some(pay);
    }

    pub fn complete(&self, completion: Completion) {
        {
            let mut held = self.completion.write();
            if held.is_some() {
                return;
            }
            *held = Some(completion.clone());
        }
        let pay = self.pay.write().take();
        let Some(pay) = pay else { return };
        #[cfg(feature = "std")]
        {
            if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                pay(Outcome::Settled(completion));
            })) {
                self.pay_errors.set(self.pay_errors.get() + 1);
                *self.last_pay_error.borrow_mut() = Some(panic_message(payload.as_ref()));
                std::panic::resume_unwind(payload);
            }
        }
        #[cfg(not(feature = "std"))]
        pay(Outcome::Settled(completion));
    }

    #[cfg(feature = "std")]
    pub fn pay_errors(&self) -> usize {
        self.pay_errors.get()
    }

    #[cfg(feature = "std")]
    pub fn last_pay_error(&self) -> Option<String> {
        self.last_pay_error.borrow().clone()
    }
}

#[cfg(feature = "std")]
fn panic_message(payload: &(dyn core::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        String::from("panic")
    }
}

pub fn settle(root: &Rc<Envelope>, body: &ResultBody, request_id: &EnvelopeId) {
    let Some(call) = root.payload.downcast_ref::<Call>() else {
        return;
    };
    let chosen = call
        .request_id
        .read()
        .clone()
        .unwrap_or_else(|| request_id.clone());
    call.complete(Completion {
        request_id: chosen,
        body: Some(body.clone()),
    });
}

pub fn settled(envelope: &Rc<Envelope>) -> Option<Completion> {
    let call = envelope.payload.downcast_ref::<Call>()?;
    call.completion.read().clone()
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
    fn begin_effects(&self, ctx: &Context) -> Result<Vec<Effect>, BoxError> {
        let root = ctx.root();
        let call = root
            .payload
            .downcast_ref::<Call>()
            .ok_or_else(|| Box::from(BusError("admitted root has no call".into())) as BoxError)?;
        if *call.cancelled.read() {
            call.complete(Completion {
                request_id: root.header.id.clone(),
                body: Some(err_payload("cancelled", None)?),
            });
            return Ok(Vec::new());
        }
        let ask = ctx.ask(STAGE, &call.topic, call.payload.clone(), call.timeout_ms)?;
        let Effect::Ask { intent } = &ask else {
            unreachable!()
        };
        *call.request_id.write() = Some(intent.request.header.id.clone());
        Ok(vec![ask])
    }

    fn on_stage_effects(
        &self,
        ctx: &Context,
        _stage: &str,
        completion: Rc<Completion>,
    ) -> Result<Vec<Effect>, BoxError> {
        let root = ctx.root();
        if let Some(call) = root.payload.downcast_ref::<Call>() {
            call.complete((*completion).clone());
        }
        Ok(Vec::new())
    }

    fn on_finished(&self, body: Option<&ResultBody>) -> Result<(), BoxError> {
        if let Some(body) = body {
            settle(&self.root, body, &self.root.header.id);
        }
        Ok(())
    }
}
