use std::rc::Rc;

use crate::envelope::Envelope;
use crate::mail::bus::Bus;
use crate::notify::watch::Interest;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    Idle,
    Checked,
    Advanced,
}

pub(super) struct Group {
    pub(super) interest: Rc<Interest>,
    pub(super) items: Vec<Rc<Envelope>>,
}

pub struct GroupLedger {
    pub topic: String,
    pub items: Vec<Rc<Envelope>>,
    pub interest: Rc<Interest>,
}

pub struct OutboxLedger {
    pub count: usize,
    pub candidates: Vec<String>,
    pub groups: Vec<GroupLedger>,
    pub buried: Vec<Rc<Envelope>>,
}

pub(super) fn awaited(bus: &Bus, envelope: &Rc<Envelope>) -> bool {
    bus.waiting(envelope.header.address.correlation_id())
}
