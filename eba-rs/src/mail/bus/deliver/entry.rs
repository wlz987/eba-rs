use std::rc::Rc;

use super::super::{Bus, PublishVote};
use super::envelope_kind;
use crate::envelope::topic::split_topic;
use crate::envelope::Envelope;
use crate::errors::{BoxError, MailboxFull};
use crate::mail::inbox::{can_enqueue, enqueue, mark_read};
use crate::notify::signal::AgendaHost;

impl Bus {
    pub fn publish(&self, envelope: &Rc<Envelope>) -> Result<bool, BoxError> {
        let observer = self.publish_observer.borrow().clone();
        if let Some(observer) = observer {
            match observer(envelope) {
                PublishVote::Pass => {}
                PublishVote::Silent => return Ok(false),
                PublishVote::Unreported => return Ok(true),
                PublishVote::Fault(error) => return Err(error),
            }
        }
        let kind = envelope_kind(envelope);
        let parts = split_topic(&envelope.header.topic).expect("invalid topic");
        let targets = self.routes.borrow().snapshot_match(&parts);
        if targets.is_empty() {
            return Ok(false);
        }
        for box_ in &targets {
            if !can_enqueue(box_, &kind) {
                return Err(Box::from(MailboxFull("target inbox full".into())));
            }
        }
        for box_ in &targets {
            enqueue(box_, Rc::clone(envelope));
        }
        for box_ in &targets {
            mark_read(box_);
        }
        Ok(true)
    }

    pub fn deliver(&self, envelope: &Rc<Envelope>, home: &Rc<dyn AgendaHost>) {
        if self.place(envelope, home) {
            return;
        }
        self.arm_offer(Rc::clone(envelope), Rc::clone(home));
    }
}
