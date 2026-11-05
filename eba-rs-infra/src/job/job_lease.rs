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
    deadline_ms: Option<u64>,
    budget_ms: Option<u64>,
    spent_ms: u64,
    callbacks: Vec<Rc<dyn Fn()>>,
}

/// A held request's one-shot slot. The runtime settles it on the next advance.
#[derive(Clone, Default)]
pub struct JobLease {
    slot: Rc<RefCell<Slot>>,
}

impl JobLease {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_options(deadline_ms: Option<u64>, budget_ms: Option<u64>) -> Self {
        let lease = Self::default();
        {
            let mut slot = lease.slot.borrow_mut();
            slot.deadline_ms = deadline_ms;
            slot.budget_ms = budget_ms;
        }
        lease
    }

    pub fn deadline_ms(&self) -> Option<u64> {
        self.slot.borrow().deadline_ms
    }
    pub fn budget_ms(&self) -> Option<u64> {
        self.slot.borrow().budget_ms
    }
    pub fn is_expired(&self, now_ms: u64) -> bool {
        self.slot
            .borrow()
            .deadline_ms
            .is_some_and(|deadline| deadline <= now_ms)
    }

    pub fn spent_ms(&self) -> u64 {
        self.slot.borrow().spent_ms
    }

    pub fn remaining_ms(&self) -> Option<i64> {
        let slot = self.slot.borrow();
        slot.budget_ms
            .map(|budget| budget as i64 - slot.spent_ms as i64)
    }

    /// Charge work; false means the budget is exhausted.
    pub fn consume(&self, ms: u64) -> bool {
        let mut slot = self.slot.borrow_mut();
        slot.spent_ms = slot.spent_ms.saturating_add(ms);
        slot.budget_ms.is_none_or(|budget| slot.spent_ms <= budget)
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
    pub fn revoke(&self, error: impl Into<String>) -> Result<bool, BoxError> {
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

    pub fn same(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.slot, &other.slot)
    }

    pub(crate) fn take(&self) -> Option<ResultBody> {
        self.slot.borrow().body.clone()
    }

    pub(crate) fn close(&self) {
        self.slot.borrow_mut().closed = true;
    }
}
