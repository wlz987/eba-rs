use crate::prelude::*;
use alloc::rc::Rc;
use core::cell::RefCell;

use super::inbox::{can_enqueue, watch_space, RouteRevoke};
use crate::envelope::topic::split_topic;
use crate::envelope::Envelope;

use crate::notify::watch::Interest;

use super::routes::Routes;

#[derive(Clone)]
pub struct Bus {
    routes: Rc<RefCell<Routes>>,
    revoke: Rc<RouteRevoke>,
    waits: Waits,
    grant_frames: Rc<RefCell<Vec<GrantFrame>>>,
    lease_wrapper: Rc<RefCell<Option<LeaseWrapper>>>,
    publish_observer: Rc<RefCell<Option<PublishObserver>>>,
    offers: Rc<RefCell<Vec<Rc<Interest>>>>,
}

mod deliver;
mod grants;
mod lease;
mod vote;
mod waits;

pub use self::lease::{Lease, LeaseWrapper, Subscription};
pub use self::vote::{GrantFrame, PublishObserver, PublishVote};
pub use self::waits::{BusCount, Waits};

use self::deliver::envelope_kind;

impl Bus {
    pub fn can_publish(&self, envelope: &Rc<Envelope>) -> bool {
        let kind = envelope_kind(envelope);
        let parts = split_topic(&envelope.header.topic).unwrap_or_default();
        self.routes
            .borrow()
            .snapshot_match(&parts)
            .iter()
            .all(|box_| can_enqueue(box_, &kind))
    }

    pub fn ready_or_watch(&self, envelope: &Rc<Envelope>, interest: &Rc<Interest>) -> bool {
        let topic = envelope.header.topic.clone();
        let kind = envelope_kind(envelope);
        self.routes.borrow().changes.watch(&topic, interest);
        let parts = split_topic(&topic).unwrap_or_default();
        let targets = self.routes.borrow().snapshot_match(&parts);
        let mut ready = true;
        for box_ in targets {
            if !can_enqueue(&box_, &kind) {
                watch_space(&box_, interest);
                ready = false;
            }
        }
        ready
    }
}

impl Default for Bus {
    fn default() -> Self {
        Self::new()
    }
}
