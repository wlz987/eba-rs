use crate::prelude::*;
use alloc::collections::{BTreeMap, BTreeSet};
use alloc::rc::Weak;
use core::cell::RefCell;

use crate::collections::IndexedMap;
use crate::errors::BoxError;

use super::signal::{AgendaHost, Signal, TIMER};
use crate::time::{Deadlines, Schedule};

pub type WakeFn = Box<dyn Fn() -> Result<(), BoxError>>;

pub struct Agenda {
    signals: RefCell<BTreeMap<String, Weak<Signal>>>,
    ready: RefCell<IndexedMap<String, ()>>,
    active: RefCell<BTreeSet<String>>,
    timers: RefCell<Deadlines<String>>,
    wake: RefCell<Option<WakeFn>>,
    fault: RefCell<Option<BoxError>>,
}

impl Agenda {
    pub fn new() -> Self {
        Agenda {
            signals: RefCell::new(BTreeMap::new()),
            ready: RefCell::new(IndexedMap::new()),
            active: RefCell::new(BTreeSet::new()),
            timers: RefCell::new(Deadlines::new()),
            wake: RefCell::new(None),
            fault: RefCell::new(None),
        }
    }

    pub fn bind_wake(&self, wake: Option<WakeFn>) {
        *self.wake.borrow_mut() = wake;
        *self.fault.borrow_mut() = None;
    }

    pub fn wake_bound(&self) -> bool {
        self.wake.borrow().is_some()
    }

    pub fn wake_fn(&self) {
        let wake = self.wake.borrow();
        let Some(wake) = &*wake else {
            return;
        };
        if let Err(error) = wake() {
            *self.fault.borrow_mut() = Some(error);
        }
    }

    pub fn take_fault(&self) -> Option<BoxError> {
        self.fault.borrow_mut().take()
    }

    pub fn is_ready(&self, actor: &str) -> bool {
        self.ready.borrow().contains_key(&actor.to_string())
    }

    pub fn ready_size(&self) -> usize {
        self.ready.borrow().len()
    }

    pub fn pop(&self) -> Option<String> {
        let mut ready = self.ready.borrow_mut();
        let key = ready.first_key().cloned()?;
        ready.remove(&key);
        Some(key)
    }

    pub fn due(&self, now_ms: u64) {
        loop {
            let actor = {
                let timers = self.timers.borrow();
                timers.peek_due(now_ms).cloned()
            };
            let Some(actor) = actor else {
                return;
            };
            self.timers.borrow_mut().remove(&actor);
            let weak = self.signals.borrow().get(&actor).cloned();
            if let Some(weak) = weak {
                if let Some(signal) = weak.upgrade() {
                    signal.notify(TIMER);
                }
            }
        }
    }

    pub fn update(&self, actor: &str, deadline: Option<u64>, active: bool, ready: bool) {
        let mut timers = self.timers.borrow_mut();
        if timers.at(&actor.to_string()) != deadline {
            timers.remove(&actor.to_string());
            if let Some(deadline) = deadline {
                timers.add(actor.to_string(), deadline);
            }
        }
        drop(timers);
        let mut active_set = self.active.borrow_mut();
        if active {
            active_set.insert(actor.to_string());
        } else {
            active_set.remove(actor);
        }
        drop(active_set);
        if ready {
            self.enqueue(actor);
        } else {
            self.unqueue(actor);
        }
    }

    pub fn remove(&self, actor: &str) {
        self.unqueue(actor);
        self.active.borrow_mut().remove(actor);
        self.timers.borrow_mut().remove(&actor.to_string());
        self.signals.borrow_mut().remove(actor);
    }

    pub fn schedule(&self, now_ms: u64) -> Schedule {
        let deadline = self.timers.borrow().next_ms();
        Schedule::new(
            !self.ready.borrow().is_empty() || deadline.is_some_and(|d| d <= now_ms),
            deadline,
            self.active.borrow().is_empty(),
        )
    }
}

impl AgendaHost for Agenda {
    fn enqueue(&self, actor: &str) {
        self.active.borrow_mut().insert(actor.to_string());
        self.ready.borrow_mut().insert(actor.to_string(), ());
        self.wake_fn();
    }

    fn unqueue(&self, actor: &str) {
        self.ready.borrow_mut().remove(&actor.to_string());
    }

    fn bind_signal(&self, actor: &str, signal: Weak<Signal>) {
        self.signals.borrow_mut().insert(actor.to_string(), signal);
    }

    fn has_signal(&self, actor: &str) -> bool {
        self.signals.borrow().contains_key(actor)
    }
}

impl Default for Agenda {
    fn default() -> Self {
        Self::new()
    }
}
