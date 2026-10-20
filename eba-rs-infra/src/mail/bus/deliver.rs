use crate::prelude::*;
use alloc::rc::Rc;
use core::cell::RefCell;

use super::{Bus, Interest};
use crate::collections::IndexedMap;
use crate::envelope::header::Address;
use crate::envelope::topic::split_topic;
use crate::envelope::Envelope;

use crate::mail::inbox::{can_enqueue, enqueue, mark_read, read_signal, watch_space, Inbox};
use crate::notify::signal::{AgendaHost, Signal};

mod entry;

impl Bus {
    fn elsewhere(&self, box_: &Rc<Inbox>, home: &Rc<dyn AgendaHost>) -> bool {
        let signal = read_signal(box_);
        !signal.bound_to(home) && !box_.is_closed()
    }

    fn foreign_targets(
        &self,
        envelope: &Rc<Envelope>,
        home: &Rc<dyn AgendaHost>,
    ) -> Vec<Rc<Inbox>> {
        let parts = split_topic(&envelope.header.topic).expect("invalid topic");
        self.routes
            .borrow()
            .snapshot_match(&parts)
            .into_iter()
            .filter(|box_| self.elsewhere(box_, home))
            .collect()
    }

    fn place(&self, envelope: &Rc<Envelope>, home: &Rc<dyn AgendaHost>) -> bool {
        let targets = self.foreign_targets(envelope, home);
        let kind = envelope_kind(envelope);
        if !targets.iter().all(|box_| can_enqueue(box_, &kind)) {
            return false;
        }
        for box_ in &targets {
            enqueue(box_, Rc::clone(envelope));
        }
        for box_ in &targets {
            mark_read(box_);
        }
        true
    }

    fn arm_offer(&self, envelope: Rc<Envelope>, home: Rc<dyn AgendaHost>) {
        let interest = Rc::new(Interest::new(
            Rc::new(Signal::new()),
            Rc::new(RefCell::new(IndexedMap::new())),
            envelope.header.topic.clone(),
        ));
        let bus = self.clone();
        let slot = Rc::clone(&envelope);
        let weak = Rc::downgrade(&interest);
        let keeper = Rc::clone(&home);
        interest.set_on_notify(Rc::new(move || {
            let Some(interest) = weak.upgrade() else {
                return;
            };
            bus.finish_offer(&slot, &keeper, &interest);
        }));
        self.offers.borrow_mut().push(Rc::clone(&interest));
        self.arm(&interest, &envelope, &home);
    }

    fn arm(&self, interest: &Rc<Interest>, envelope: &Rc<Envelope>, home: &Rc<dyn AgendaHost>) {
        let kind = envelope_kind(envelope);
        for box_ in self.foreign_targets(envelope, home) {
            if !can_enqueue(&box_, &kind) {
                watch_space(&box_, interest);
            }
        }
    }

    fn finish_offer(
        &self,
        envelope: &Rc<Envelope>,
        home: &Rc<dyn AgendaHost>,
        interest: &Rc<Interest>,
    ) {
        if !self.place(envelope, home) {
            self.arm(interest, envelope, home);
            return;
        }
        self.offers
            .borrow_mut()
            .retain(|held| !Rc::ptr_eq(held, interest));
        interest.dispose();
    }
}

pub(super) fn envelope_kind(envelope: &Rc<Envelope>) -> String {
    match envelope.header.address {
        Address::Event => "event",
        Address::Request { .. } => "request",
        Address::Result { .. } => "result",
        Address::Diagnostic => "diagnostic",
    }
    .to_string()
}
