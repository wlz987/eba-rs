use crate::collections::IndexedMap;
use crate::prelude::*;
use alloc::rc::Rc;
use core::cell::RefCell;

use crate::envelope::Envelope;
use crate::notify::signal::{Signal, DELIVERY};
use crate::notify::{Interest, Watchers};
use crate::time::numbers::positive_int;

mod ledger;
mod queue;

use self::ledger::{awaited, TopicQueue};
pub use self::ledger::{MailboxLedger, Step, TopicLedger};

pub struct Mailbox {
    limit: usize,
    signal: Rc<Signal>,
    candidates: Rc<RefCell<IndexedMap<String, ()>>>,
    groups: IndexedMap<String, TopicQueue>,
    count: usize,
    space: Rc<Watchers>,
}

impl Mailbox {
    pub fn new(limit: usize, signal: Rc<Signal>) -> Self {
        Mailbox {
            limit: positive_int(limit, "mailbox_limit"),
            signal,
            candidates: Rc::new(RefCell::new(IndexedMap::new())),
            groups: IndexedMap::new(),
            count: 0,
            space: Rc::new(Watchers::new()),
        }
    }

    pub fn watch_space(&self, interest: &Rc<Interest>) {
        self.space.watch(interest);
    }

    fn notify_space(&self) {
        if self.has_room() {
            self.space.notify();
        }
    }

    pub fn size(&self) -> usize {
        self.count
    }

    pub fn inspect(&self) -> MailboxLedger {
        MailboxLedger {
            count: self.count,
            candidates: self.candidates.borrow().keys().cloned().collect(),
            groups: self
                .groups
                .iter()
                .map(|(topic, group)| TopicLedger {
                    topic: topic.clone(),
                    items: group.items.clone(),
                    interest: Rc::clone(&group.interest),
                })
                .collect(),
        }
    }

    fn queued(&self) -> usize {
        self.count
    }

    pub fn has_room(&self) -> bool {
        self.queued() < self.limit
    }

    pub fn contains(&self, envelope: &Rc<Envelope>) -> bool {
        self.groups.values().any(|group| {
            group
                .items
                .iter()
                .any(|item| item.header.id == envelope.header.id)
        })
    }

    pub fn rearm(&mut self, envelope: &Rc<Envelope>) {
        let topic = envelope.header.topic.clone();
        {
            let group = match self.groups.get(&topic) {
                Some(group) => group,
                None => return,
            };
            if group
                .items
                .first()
                .is_none_or(|head| head.header.id != envelope.header.id)
            {
                return;
            }
        }
        self.add_candidate(&topic);
        self.signal.notify(DELIVERY);
    }

    pub fn bury(&mut self, envelope: &Rc<Envelope>) {
        let topic = envelope.header.topic.clone();
        let Some(group) = self.groups.get(&topic) else {
            return;
        };
        if group
            .items
            .first()
            .is_none_or(|head| head.header.id != envelope.header.id)
        {
            return;
        }
        let interest = Rc::clone(&group.interest);
        self.remove_candidate(&topic);
        interest.dispose();
        let moved = match self.groups.get_mut(&topic) {
            Some(group) => {
                let moved = group.items.remove(0);
                if group.items.is_empty() {
                    self.groups.remove(&topic);
                }
                Some(moved)
            }
            None => None,
        };
        if let Some(_moved) = moved {
            self.count -= 1;
            self.notify_space();
        }
    }
}
