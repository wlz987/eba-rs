use std::cell::RefCell;

use std::rc::Rc;

use crate::envelope::ids::ActorId;
use crate::envelope::Envelope;
use crate::errors::{combine_failures, BoxError, BusError};
use crate::mail::bus::{Bus, Lease};
use crate::mail::inbox::{Inbox, Receiver};
use crate::pattern::parse_pattern;

enum SubState {
    Unset,
    Changing,
    Active(Vec<Rc<dyn Lease>>),
}

fn taken(state: &SubState) -> Option<Vec<Rc<dyn Lease>>> {
    match state {
        SubState::Active(list) => Some(list.clone()),
        _ => None,
    }
}

pub struct Subscriber {
    pub actor_id: ActorId,
    pub patterns: Vec<String>,
    pub receiver: Receiver,
    pub inbox: Rc<Inbox>,
    subscriptions: RefCell<SubState>,
    owed: RefCell<Vec<Rc<dyn Lease>>>,
}

impl Subscriber {
    pub fn new(actor: ActorId, receiver: Receiver, patterns: Vec<String>) -> Self {
        for item in &patterns {
            parse_pattern(item).expect("invalid pattern");
        }
        let receiver = receiver.take();
        let inbox = receiver.inbox.clone();
        Subscriber {
            actor_id: actor,
            patterns,
            receiver,
            inbox,
            subscriptions: RefCell::new(SubState::Unset),
            owed: RefCell::new(Vec::new()),
        }
    }

    pub fn start(&self, bus: &Bus) -> Result<(), BoxError> {
        if matches!(&*self.subscriptions.borrow(), SubState::Active(_)) {
            return Err(Box::from(BusError("subscriber already started".into())));
        }
        if matches!(&*self.subscriptions.borrow(), SubState::Changing) {
            return Err(Box::from(BusError("reentrant subscriber mutation".into())));
        }
        if self.inbox.is_closed() {
            return Err(Box::from(BusError("inbox closed".into())));
        }
        let release_receiver = self.receiver.borrow();
        *self.subscriptions.borrow_mut() = SubState::Changing;
        let mut subscriptions: Vec<Rc<dyn Lease>> = Vec::new();
        let built = (|| -> Result<(), BoxError> {
            release(&mut self.owed.borrow_mut())?;
            for pattern in &self.patterns {
                subscriptions.push(bus.subscribe_lease(pattern, &self.inbox)?);
            }
            if self.inbox.is_closed() {
                return Err(Box::from(BusError("inbox closed".into())));
            }
            Ok(())
        })();
        let settled = match built {
            Ok(()) => {
                *self.subscriptions.borrow_mut() = SubState::Active(subscriptions);
                Ok(())
            }
            Err(error) => match release(&mut subscriptions) {
                Ok(()) => {
                    *self.subscriptions.borrow_mut() = SubState::Unset;
                    Err(error)
                }
                Err(secondary) => {
                    self.owed.borrow_mut().extend(subscriptions);
                    *self.subscriptions.borrow_mut() = SubState::Unset;
                    Err(combine_failures(error, secondary))
                }
            },
        };
        release_receiver.release();
        settled
    }

    pub fn stop(&self) -> Result<(), BoxError> {
        self.release_(false)
    }

    pub fn close(&self, _bus: &Bus) -> Result<(), BoxError> {
        self.release_(true)
    }

    pub fn try_recv(&self) -> Result<Option<Rc<Envelope>>, BoxError> {
        self.receiver.try_recv()
    }

    fn release_(&self, closing: bool) -> Result<(), BoxError> {
        if matches!(&*self.subscriptions.borrow(), SubState::Changing) {
            return Err(Box::from(BusError("reentrant subscriber mutation".into())));
        }
        let previous = std::mem::replace(&mut *self.subscriptions.borrow_mut(), SubState::Changing);
        let mut subscriptions = taken(&previous).unwrap_or_default();
        let failure = release(&mut subscriptions)
            .err()
            .or_else(|| release(&mut self.owed.borrow_mut()).err());
        if failure.is_none() && closing {
            self.receiver.close();
        }
        *self.subscriptions.borrow_mut() = if subscriptions.is_empty() {
            SubState::Unset
        } else {
            SubState::Active(subscriptions)
        };
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

fn release(subscriptions: &mut Vec<Rc<dyn Lease>>) -> Result<(), BoxError> {
    while let Some(last) = subscriptions.last() {
        let lease = Rc::clone(last);
        lease.close()?;
        subscriptions.pop();
    }
    Ok(())
}
