pub(crate) mod subscriptions;

use crate::bus::subscriptions::Subscriptions;
use crate::envelope::{split_topic, Envelope};
use crate::inbox::InboxHandle;
use crate::pattern::parse_pattern;
use crate::{Error, Result};
use std::cell::RefCell;
use std::rc::Rc;

pub type BusHandle = Rc<Bus>;

#[derive(Debug, Default)]
pub struct Bus {
    subs: RefCell<Subscriptions>,
}

impl Bus {
    pub fn new() -> BusHandle {
        Rc::new(Bus::default())
    }

    pub fn subscribe(&self, pattern: &str, inbox: &InboxHandle) -> Result<()> {
        let pat = parse_pattern(pattern)?;
        self.subs.borrow_mut().subscribe(&pat, inbox);
        Ok(())
    }

    pub fn unsubscribe(&self, pattern: &str, inbox: &InboxHandle) -> Result<bool> {
        let pat = parse_pattern(pattern)?;
        Ok(self.subs.borrow_mut().unsubscribe(&pat, inbox))
    }

    pub fn publish(&self, e: &Envelope) -> Result<()> {
        let parts = split_topic(&e.header.topic).map_err(Error::from)?;
        let targets: Vec<InboxHandle> = self
            .subs
            .borrow()
            .snapshot_match(&parts)
            .into_iter()
            .filter(|box_| !box_.is_closed())
            .collect();
        if targets.is_empty() {
            return Ok(());
        }
        for box_ in &targets {
            if !box_.has_room() {
                return Err(Error::MailboxFull);
            }
        }
        let mut enqueued: Vec<&InboxHandle> = Vec::with_capacity(targets.len());
        for box_ in &targets {
            if !box_.try_enqueue(e.clone()) {
                for done in &enqueued {
                    done.try_drop_last(e);
                }
                return Err(Error::MailboxFull);
            }
            enqueued.push(box_);
        }
        Ok(())
    }
}
