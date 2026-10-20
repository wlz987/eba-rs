use crate::prelude::*;
use alloc::rc::Rc;

use crate::envelope::Envelope;
use crate::errors::{BoxError, DeliveryError, MailboxFull};
use crate::mail::bus::Bus;
use crate::notify::signal::DELIVERY;
use crate::notify::watch::Interest;

use super::{awaited, Group, Outbox, Step};

impl Outbox {
    pub fn send(&mut self, bus: &Bus, envelope: Rc<Envelope>) -> Result<(), BoxError> {
        let topic = envelope.header.topic.clone();
        let existing = self.groups.contains_key(&topic);
        if !existing {
            match bus.publish(&envelope) {
                Ok(delivered) => {
                    if !delivered && awaited(bus, &envelope) {
                        return Err(Box::new(DeliveryError::new(
                            alloc::sync::Arc::new((*envelope).clone()),
                            "result publication failed",
                        )));
                    }
                    return Ok(());
                }
                Err(error) => {
                    if error.downcast_ref::<MailboxFull>().is_none() {
                        let delivery = DeliveryError::caused(
                            alloc::sync::Arc::new((*envelope).clone()),
                            "result publication failed",
                            error,
                        );
                        return Err(Box::from(delivery));
                    }
                }
            }
        }
        if self.queued() >= self.limit {
            let delivery = DeliveryError::outbox_full(alloc::sync::Arc::new((*envelope).clone()));
            return Err(Box::from(delivery));
        }
        let first = !existing;
        if !existing {
            let interest = Rc::new(Interest::new(
                Rc::clone(&self.signal),
                Rc::clone(&self.candidates),
                topic.clone(),
            ));
            self.groups.insert(
                topic.clone(),
                Group {
                    interest,
                    items: Vec::new(),
                },
            );
        }
        let group = self.groups.get_mut(&topic).unwrap();
        group.items.push(envelope);
        self.count += 1;
        if !first {
            return Ok(());
        }
        let interest = Rc::clone(&group.interest);
        let envelope = group.items[0].clone();
        if bus.ready_or_watch(&envelope, &interest) {
            self.add_candidate(&topic);
            self.signal.notify(DELIVERY);
        }
        Ok(())
    }

    pub fn flush_one(&mut self, bus: &Bus) -> Result<(Step, Option<Rc<Envelope>>), BoxError> {
        let Some(topic) = self.candidates.borrow().first_key().cloned() else {
            self.signal.clear(DELIVERY);
            return Ok((Step::Idle, None));
        };
        self.remove_candidate(&topic);
        if !self.candidates.borrow().is_empty() {
            self.signal.notify(DELIVERY);
        } else {
            self.signal.clear(DELIVERY);
        }
        let Some(group) = self.groups.get_mut(&topic) else {
            return Ok((Step::Idle, None));
        };
        let interest = Rc::clone(&group.interest);
        let envelope = group.items[0].clone();
        interest.dispose();
        if !bus.ready_or_watch(&envelope, &interest) {
            return Ok((Step::Checked, None));
        }
        let delivered = match bus.publish(&envelope) {
            Ok(delivered) => delivered,
            Err(error) => {
                if error.downcast_ref::<MailboxFull>().is_some() {
                    if bus.ready_or_watch(&envelope, &interest) {
                        self.add_candidate(&topic);
                        self.signal.notify(DELIVERY);
                    }
                    return Ok((Step::Checked, None));
                }
                interest.dispose();
                return Err(Box::new(DeliveryError::caused(
                    alloc::sync::Arc::new((*envelope).clone()),
                    "result publication failed",
                    error,
                )));
            }
        };
        if !delivered && awaited(bus, &envelope) {
            interest.dispose();
            return Err(Box::new(DeliveryError::new(
                alloc::sync::Arc::new((*envelope).clone()),
                "result publication failed",
            )));
        }
        group.items.remove(0);
        self.count -= 1;
        if group.items.is_empty() {
            interest.dispose();
            self.groups.remove(&topic);
        } else {
            self.add_candidate(&topic);
        }
        if !self.candidates.borrow().is_empty() {
            self.signal.notify(DELIVERY);
        } else {
            self.signal.clear(DELIVERY);
        }
        Ok((Step::Advanced, Some(envelope)))
    }

    pub fn drain(&mut self) -> Vec<Rc<Envelope>> {
        let mut envelopes: Vec<Rc<Envelope>> = Vec::new();
        for (_, group) in self.groups.take_all() {
            group.interest.dispose();
            for item in group.items {
                envelopes.push(item);
            }
        }
        envelopes.append(&mut self.buried);
        self.candidates.borrow_mut().clear();
        self.signal.clear(DELIVERY);
        self.count = 0;
        envelopes
    }

    pub(super) fn add_candidate(&mut self, topic: &str) {
        self.candidates.borrow_mut().insert(topic.to_string(), ());
    }

    pub(super) fn remove_candidate(&mut self, topic: &str) {
        self.candidates.borrow_mut().remove(&topic.to_string());
    }
}
