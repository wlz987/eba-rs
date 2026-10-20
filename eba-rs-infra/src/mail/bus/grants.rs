use crate::prelude::*;
use alloc::rc::Rc;
use core::cell::RefCell;

use crate::mail::inbox::{Inbox, RouteRevoke};
use crate::mail::routes::Routes;

use super::{Bus, BusCount, GrantFrame, Lease, LeaseWrapper, PublishObserver, Waits};

mod subscribe;
mod view;

impl Bus {
    pub fn new() -> Self {
        let routes = Rc::new(RefCell::new(Routes::new()));
        let handle = Rc::clone(&routes);
        let changes = Rc::clone(&routes.borrow().changes);
        Bus {
            revoke: RouteRevoke::new(move |inbox| {
                let revoked = handle.borrow_mut().unsubscribe_all(inbox);
                for pattern in revoked {
                    changes.notify(&pattern);
                }
            }),
            routes,
            waits: Waits::default(),
            grant_frames: Rc::new(RefCell::new(Vec::new())),
            lease_wrapper: Rc::new(RefCell::new(None)),
            publish_observer: Rc::new(RefCell::new(None)),
            offers: Rc::new(RefCell::new(Vec::new())),
        }
    }

    pub fn wrap_leases(&self, wrapper: Option<LeaseWrapper>) {
        *self.lease_wrapper.borrow_mut() = wrapper;
    }

    /// Install the seam that lets a test break a publication.
    pub fn observe_publish(&self, observer: Option<PublishObserver>) {
        *self.publish_observer.borrow_mut() = observer;
    }

    pub fn counter(&self) -> BusCount {
        BusCount {
            routes: Rc::clone(&self.routes),
            waits: self.waits.clone(),
        }
    }

    pub fn open_grants(&self) -> usize {
        self.grant_frames.borrow_mut().push(Vec::new());
        self.grant_frames.borrow().len() - 1
    }

    pub fn close_grants(&self) {
        self.grant_frames.borrow_mut().pop();
    }

    pub fn note_grant(&self, inbox: &Rc<Inbox>, granted: Rc<dyn Lease>) {
        if let Some(frame) = self.grant_frames.borrow_mut().last_mut() {
            let native = frame
                .iter()
                .rposition(|(id, lease)| *id == inbox.id && lease.subscription_ptr().is_some());
            match native {
                Some(slot) => frame[slot].1 = granted,
                None => frame.push((inbox.id, granted)),
            }
        }
    }

    pub fn grant_frame(&self, depth: usize) -> GrantFrame {
        self.grant_frames
            .borrow()
            .get(depth)
            .cloned()
            .unwrap_or_default()
    }
}
