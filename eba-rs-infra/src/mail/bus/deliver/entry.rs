use crate::prelude::*;
use alloc::rc::Rc;

use super::super::{Bus, PublishVote};
use super::envelope_kind;
use crate::envelope::topic::split_topic;
use crate::envelope::Envelope;
use crate::errors::{BoxError, EnvelopeBuildError, MailboxFull};
use crate::mail::inbox::{can_enqueue, enqueue, mark_read, replace_topic};
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

    /// 尽力而为地扇出：满或已关的订阅者跳过，返回未送到的只数；观测者的票仍然算数。
    pub fn publish_best_effort(&self, envelope: &Rc<Envelope>) -> Result<usize, BoxError> {
        let kind = envelope_kind(envelope);
        let parts = split_topic(&envelope.header.topic).expect("invalid topic");
        let targets = self.routes.borrow().snapshot_match(&parts);
        if let Some(observer) = self.publish_observer.borrow().clone() {
            match observer(envelope) {
                PublishVote::Pass => {}
                PublishVote::Silent => return Ok(targets.len()),
                PublishVote::Unreported => return Ok(0),
                PublishVote::Fault(error) => return Err(error),
            }
        }
        let total = targets.len();
        let ready: Vec<_> = targets
            .into_iter()
            .filter(|box_| can_enqueue(box_, &kind))
            .collect();
        for box_ in &ready {
            enqueue(box_, Rc::clone(envelope));
        }
        for box_ in &ready {
            mark_read(box_);
        }
        Ok(total - ready.len())
    }

    /// 每个目标只保留同一 topic 的最新一封；返回没接住的目标数。
    pub fn publish_latest(&self, envelope: &Rc<Envelope>) -> Result<usize, BoxError> {
        if envelope.header.address.kind() != "event" {
            return Err(Box::from(EnvelopeBuildError(
                "publish_latest requires an event".into(),
            )));
        }
        let parts = split_topic(&envelope.header.topic).expect("invalid topic");
        let targets = self.routes.borrow().snapshot_match(&parts);
        if let Some(observer) = self.publish_observer.borrow().clone() {
            match observer(envelope) {
                PublishVote::Pass => {}
                PublishVote::Silent => return Ok(targets.len()),
                PublishVote::Unreported => return Ok(0),
                PublishVote::Fault(error) => return Err(error),
            }
        }
        let total = targets.len();
        let mut accepted = Vec::new();
        for box_ in targets {
            if replace_topic(&box_, Rc::clone(envelope)) {
                accepted.push(box_);
            }
        }
        for box_ in &accepted {
            mark_read(box_);
        }
        Ok(total - accepted.len())
    }

    pub fn deliver(&self, envelope: &Rc<Envelope>, home: &Rc<dyn AgendaHost>) {
        if self.place(envelope, home) {
            return;
        }
        self.arm_offer(Rc::clone(envelope), Rc::clone(home));
    }
}
