use std::cell::Cell;
use std::rc::{Rc, Weak};
use std::sync::atomic::Ordering;

use crate::envelope::Envelope;
use crate::errors::{BoxError, BusError};

use super::core::Inbox;
use super::route::RouteRevoke;
use super::wakers::Wakers;
use super::CLOSES;

pub struct Receiver {
    pub inbox: Rc<Inbox>,
    pub(super) active: Cell<bool>,
    pub(super) borrows: Cell<usize>,
}

impl Receiver {
    pub fn is_active(&self) -> bool {
        self.active.get()
    }

    pub fn borrow(&self) -> ReceiverBorrow<'_> {
        self.require_active();
        self.borrows.set(self.borrows.get() + 1);
        ReceiverBorrow {
            target: self,
            released: Cell::new(false),
        }
    }

    pub fn take(&self) -> Receiver {
        self.require_active();
        if self.borrows.get() != 0 {
            panic!("receiver is borrowed");
        }
        let transferred = Receiver {
            inbox: Rc::clone(&self.inbox),
            active: Cell::new(true),
            borrows: Cell::new(0),
        };
        self.active.set(false);
        transferred
    }

    pub fn try_recv(&self) -> Result<Option<Rc<Envelope>>, BoxError> {
        if !self.active.get() {
            return Err(Box::new(BusError("receiver transferred or closed".into())) as BoxError);
        }
        let mut wakers = Wakers::none();
        let envelope = {
            let mut state = self.inbox.state.borrow_mut();
            let full = state.items.len() == self.inbox.capacity;
            let envelope = state.items.pop_front();
            if envelope.is_some() {
                wakers = Wakers::of(&state);
                wakers.cleared = state.items.is_empty();
                wakers.freed = full;
            }
            envelope
        };
        wakers.run();
        Ok(envelope)
    }

    pub fn close(&self) -> bool {
        let mut state = self.inbox.state.borrow_mut();
        let started = self.active.get();
        let mut wakers = Wakers::none();
        if started {
            self.active.set(false);
            state.closed = true;
            CLOSES.fetch_add(1, Ordering::Relaxed);
            let seized = std::mem::take(&mut state.items);
            state.seized.extend(seized);
            wakers = Wakers::of(&state);
            wakers.cleared = true;
            wakers.freed = true;
        } else if !state.closed {
            return false;
        }
        let owners: Vec<Rc<RouteRevoke>> = state.routes.iter().filter_map(Weak::upgrade).collect();
        drop(state);
        wakers.run();
        for owner in owners {
            let discarded = DiscardOwner {
                inbox: &self.inbox,
                owner: Rc::clone(&owner),
            };
            owner.unsubscribe_all(&self.inbox);
            drop(discarded);
        }
        started
    }

    fn require_active(&self) {
        if !self.active.get() {
            panic!("receiver transferred or closed");
        }
    }
}

struct DiscardOwner<'a> {
    inbox: &'a Rc<Inbox>,
    owner: Rc<RouteRevoke>,
}

impl Drop for DiscardOwner<'_> {
    fn drop(&mut self) {
        let mut state = self.inbox.state.borrow_mut();
        state.routes.retain(|weak| {
            weak.upgrade()
                .map(|live| !RouteRevoke::same(&live, &self.owner))
                .unwrap_or(false)
        });
    }
}

pub struct ReceiverBorrow<'a> {
    target: &'a Receiver,
    released: Cell<bool>,
}

impl<'a> ReceiverBorrow<'a> {
    pub(crate) fn release(&self) {
        if !self.released.get() {
            self.target.borrows.set(self.target.borrows.get() - 1);
            self.released.set(true);
        }
    }
}

impl<'a> Drop for ReceiverBorrow<'a> {
    fn drop(&mut self) {
        self.release();
    }
}
