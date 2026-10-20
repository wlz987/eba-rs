use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use super::Bus;
use crate::envelope::topic::split_topic;
use crate::envelope::EnvelopeId;
use crate::mail::inbox::Inbox;
use crate::mail::routes::Routes;

#[derive(Clone, Default)]
pub struct Waits {
    pending: Rc<RefCell<HashSet<EnvelopeId>>>,
}

impl Waits {
    fn hold(&self, request_id: &EnvelopeId) {
        self.pending.borrow_mut().insert(request_id.clone());
    }

    fn release(&self, request_id: &EnvelopeId) {
        self.pending.borrow_mut().remove(request_id);
    }

    fn waiting(&self, request_id: Option<&EnvelopeId>) -> bool {
        request_id.is_some_and(|id| self.pending.borrow().contains(id))
    }
}

#[derive(Clone)]
pub struct BusCount {
    pub(super) routes: Rc<RefCell<Routes>>,
    pub(super) waits: Waits,
}

impl BusCount {
    pub fn subscription_count(&self, topic: &str, inbox: &Rc<Inbox>) -> usize {
        let parts = split_topic(topic).unwrap_or_default();
        self.routes.borrow().hold_count(&parts, inbox)
    }

    pub fn hold_wait(&self, request_id: &EnvelopeId) {
        self.waits.hold(request_id);
    }

    pub fn release_wait(&self, request_id: &EnvelopeId) {
        self.waits.release(request_id);
    }
}

impl Bus {
    pub fn hold_wait(&self, request_id: &EnvelopeId) {
        self.waits.hold(request_id);
    }

    pub fn release_wait(&self, request_id: &EnvelopeId) {
        self.waits.release(request_id);
    }

    pub fn waiting(&self, request_id: Option<&EnvelopeId>) -> bool {
        self.waits.waiting(request_id)
    }
}
