use alloc::rc::Rc;
use core::cell::RefCell;

use crate::envelope::result::{err_payload, err_payload_code, ResultBody};
use crate::errors::{BoxError, BusError};
use crate::prelude::*;

#[derive(Default)]
struct Slot {
    body: Option<ResultBody>,
    closed: bool,
    cancelled: bool,
    callbacks: Vec<Rc<dyn Fn()>>,
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
        if slot.cancelled {
            return Ok(());
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

    /// Settle as cancelled and notify adapters. A late send is a no-op.
    pub fn cancel(&self, error: impl Into<String>) -> Result<bool, BoxError> {
        let body = err_payload(error, None)?;
        let callbacks = {
            let mut slot = self.slot.borrow_mut();
            if slot.body.is_some() || slot.closed {
                return Ok(false);
            }
            slot.body = Some(body);
            slot.cancelled = true;
            core::mem::take(&mut slot.callbacks)
        };
        for callback in callbacks {
            callback();
        }
        Ok(true)
    }

    pub fn on_cancel<F>(&self, callback: F)
    where
        F: Fn() + 'static,
    {
        let mut slot = self.slot.borrow_mut();
        if slot.cancelled {
            drop(slot);
            callback();
            return;
        }
        slot.callbacks.push(Rc::new(callback));
    }

    pub fn is_cancelled(&self) -> bool {
        self.slot.borrow().cancelled
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
