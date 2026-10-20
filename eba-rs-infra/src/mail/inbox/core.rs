use crate::prelude::*;
use ::core::cell::{Cell, RefCell};
use ::core::sync::atomic::Ordering;
use alloc::collections::VecDeque;
use alloc::rc::Rc;

use super::ops::remaining;
use super::receiver::Receiver;
use super::state::InboxState;
use super::{CLOSES, NEXT_ID};
use crate::errors::{BoxError, BusError};
use crate::notify::{Signal, Watchers, READ};

pub struct Inbox {
    pub id: usize,
    pub capacity: usize,
    pub(crate) state: Rc<RefCell<InboxState>>,
}

fn next_id() -> usize {
    NEXT_ID.fetch_add(1, Ordering::Relaxed) + 1
}

pub fn close_count() -> usize {
    CLOSES.load(Ordering::Relaxed)
}

impl Inbox {
    pub fn try_new(capacity: usize) -> Result<Rc<Inbox>, BoxError> {
        if capacity < 1 {
            return Err(
                Box::new(BusError("capacity must be a positive integer".into())) as BoxError,
            );
        }
        Ok(Self::new(capacity))
    }

    pub fn new(capacity: usize) -> Rc<Inbox> {
        let id = next_id();
        assert!(capacity >= 1, "capacity must be a positive integer");
        let state = InboxState {
            items: VecDeque::new(),
            closed: false,
            claimed: false,
            work_paused: false,
            read: Some(Rc::new(Signal::new())),
            space: Some(Rc::new(Watchers::new())),
            routes: Vec::new(),
            seized: VecDeque::new(),
        };
        Rc::new(Inbox {
            id,
            capacity,
            state: Rc::new(RefCell::new(state)),
        })
    }

    pub fn try_receiver(self: &Rc<Inbox>) -> Result<Receiver, BoxError> {
        let mut state = self.state.borrow_mut();
        if state.closed || state.claimed {
            return Err(
                Box::new(BusError("inbox unavailable or already owned".into())) as BoxError,
            );
        }
        state.claimed = true;
        Ok(Receiver {
            inbox: Rc::clone(self),
            active: Cell::new(true),
            borrows: Cell::new(0),
        })
    }

    pub fn receiver(self: &Rc<Inbox>) -> Receiver {
        self.try_receiver()
            .expect("inbox unavailable or already owned")
    }

    pub fn is_closed(&self) -> bool {
        self.state.borrow().closed
    }

    pub fn has_read(&self) -> bool {
        self.state
            .borrow()
            .read
            .as_ref()
            .is_some_and(|s| s.has(READ))
    }

    pub fn remaining(&self) -> usize {
        remaining(self)
    }
}
