use crate::collections::IndexedMap;
use std::cell::RefCell;
use std::rc::Rc;

use crate::envelope::Envelope;
use crate::notify::signal::{Signal, DELIVERY};
use crate::time::numbers::positive_int;

mod ledger;
mod queue;

use self::ledger::{awaited, Group};
pub use self::ledger::{GroupLedger, OutboxLedger, Step};

pub struct Outbox {
    limit: usize,
    signal: Rc<Signal>,
    candidates: Rc<RefCell<IndexedMap<String, ()>>>,
    groups: IndexedMap<String, Group>,
    buried: Vec<Rc<Envelope>>,
    count: usize,
}

impl Outbox {
    pub fn new(limit: usize, signal: Rc<Signal>) -> Self {
        Outbox {
            limit: positive_int(limit, "outbox_limit"),
            signal,
            candidates: Rc::new(RefCell::new(IndexedMap::new())),
            groups: IndexedMap::new(),
            buried: Vec::new(),
            count: 0,
        }
    }

    pub fn size(&self) -> usize {
        self.count
    }

    pub fn buried(&self) -> usize {
        self.buried.len()
    }

    fn remember_buried(&mut self, envelope: Rc<Envelope>) {
        self.buried.push(envelope);
        while self.buried.len() > self.limit {
            self.buried.remove(0);
            self.count -= 1;
        }
    }

    pub fn inspect(&self) -> OutboxLedger {
        OutboxLedger {
            count: self.count,
            candidates: self.candidates.borrow().keys().cloned().collect(),
            groups: self
                .groups
                .iter()
                .map(|(topic, group)| GroupLedger {
                    topic: topic.clone(),
                    items: group.items.clone(),
                    interest: Rc::clone(&group.interest),
                })
                .collect(),
            buried: self.buried.clone(),
        }
    }

    fn queued(&self) -> usize {
        self.count - self.buried.len()
    }

    pub fn has_room(&self) -> bool {
        self.queued() < self.limit
    }

    pub fn contains(&self, envelope: &Rc<Envelope>) -> bool {
        if self
            .buried
            .iter()
            .any(|item| item.header.id == envelope.header.id)
        {
            return true;
        }
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
        let interest;
        {
            let group = match self.groups.get(&topic) {
                Some(group) => group,
                None => {
                    if !self.contains(envelope) {
                        self.remember_buried(Rc::clone(envelope));
                        self.count += 1;
                    }
                    return;
                }
            };
            if group
                .items
                .first()
                .is_none_or(|head| head.header.id != envelope.header.id)
            {
                if !self.contains(envelope) {
                    self.remember_buried(Rc::clone(envelope));
                    self.count += 1;
                }
                return;
            }
            interest = Rc::clone(&group.interest);
        }
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
        let Some(moved) = moved else { return };
        self.remember_buried(moved);
        if self.groups.contains_key(&topic) {
            self.add_candidate(&topic);
            self.signal.notify(DELIVERY);
            return;
        }
        self.signal
            .set(DELIVERY, !self.candidates.borrow().is_empty());
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::sync::Arc;

    use crate::envelope::ids::{actor_id, envelope_id};
    use crate::envelope::{make_envelope, MakeEnvelope};
    use crate::notify::signal::Signal;
    use crate::Payload;

    use super::Outbox;

    #[test]
    fn buried_is_bounded_by_outbox_limit() {
        let mut outbox = Outbox::new(3, Rc::new(Signal::new()));
        for index in 0..10u64 {
            let envelope = Rc::new(
                make_envelope(MakeEnvelope {
                    topic: "out".into(),
                    payload: Arc::new(index) as Payload,
                    from: actor_id("test"),
                    cause: None,
                    id: Some(envelope_id(format!("e{index}"))),
                    kind: "event".into(),
                    reply_to: None,
                    correlation_id: None,
                })
                .unwrap(),
            );
            outbox.bury(&envelope);
        }
        assert_eq!(outbox.buried(), 3);
        assert_eq!(outbox.size(), 3);
    }
}
