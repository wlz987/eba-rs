use alloc::rc::Rc;
use core::cell::RefCell;

use crate::envelope::result::{err_payload, err_payload_code, ResultBody};
use crate::errors::{BoxError, BusError};
use crate::prelude::*;

#[derive(Default)]
struct Slot {
    body: Option<ResultBody>,
    closed: bool,
}

/// A held request's one-shot slot. The runtime settles it on the next advance.
#[derive(Clone, Default)]
pub struct Reply {
    slot: Rc<RefCell<Slot>>,
}

impl Reply {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn send(&self, body: ResultBody) -> Result<(), BoxError> {
        let mut slot = self.slot.borrow_mut();
        if slot.closed {
            return Err(Box::from(BusError("reply closed".into())));
        }
        if slot.body.is_some() {
            return Err(Box::from(BusError("reply already settled".into())));
        }
        slot.body = Some(body);
        Ok(())
    }

    pub fn fail(&self, error: impl Into<String>, code: Option<i32>) -> Result<(), BoxError> {
        let body = match code {
            Some(code) => err_payload_code(error, code, None)?,
            None => err_payload(error, None)?,
        };
        self.send(body)
    }

    pub fn is_closed(&self) -> bool {
        self.slot.borrow().closed
    }

    pub(crate) fn take(&self) -> Option<ResultBody> {
        self.slot.borrow().body.clone()
    }

    pub(crate) fn close(&self) {
        self.slot.borrow_mut().closed = true;
    }
}
