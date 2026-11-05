use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::{Rc, Weak};

use crate::mail::inbox::Inbox;
use crate::notify::watch::Interest;
use crate::runtime::Runtime;
use crate::wake::Wake;

use super::{Drive, Inner};

impl Drive {
    pub fn new(runtime: Runtime) -> Rc<Drive> {
        let drive = Rc::new(Drive {
            runtime: Rc::new(RefCell::new(runtime)),
            inner: Rc::new(RefCell::new(Inner::default())),
            wake: RefCell::new(None),
            task: RefCell::new(None),
            closed: Rc::new(Cell::new(false)),
            pumping: Rc::new(Cell::new(false)),
            releasing: Rc::new(Cell::new(false)),
            fault: Rc::new(RefCell::new(None)),
            slf: RefCell::new(Weak::new()),
            space_kicks: RefCell::new(HashMap::new()),
            laps: Cell::new(0),
        });
        *drive.slf.borrow_mut() = Rc::downgrade(&drive);
        drive.reset();
        drive
    }

    pub(super) fn reset(&self) {
        let inner = Rc::clone(&self.inner);
        let mut ledger = inner.borrow_mut();
        ledger.waiters.clear();
        ledger.stops.clear();
        drop(ledger);
        self.space_kicks.borrow_mut().clear();
        *self.wake.borrow_mut() = Some(Wake::new());
        self.closed.set(false);
        self.pumping.set(false);
        self.releasing.set(false);
        *self.fault.borrow_mut() = None;
    }

    pub fn runtime_ref(&self) -> Rc<RefCell<Runtime>> {
        Rc::clone(&self.runtime)
    }

    pub fn waiter_count(&self) -> usize {
        self.inner.borrow().waiters.len()
    }

    pub fn wake_token(&self) -> Option<Rc<Wake>> {
        self.wake.borrow().clone()
    }

    pub fn stop_count(&self) -> usize {
        self.inner.borrow().stops.len()
    }

    pub fn next_delay(&self) -> Option<u64> {
        self.delay()
    }

    pub fn space_watcher(&self, inbox: &Rc<Inbox>) -> Option<Rc<Interest>> {
        self.space_kicks
            .borrow()
            .get(&(Rc::as_ptr(inbox) as usize))
            .cloned()
    }

    pub fn space_watcher_count(&self) -> usize {
        self.space_kicks.borrow().len()
    }

    pub fn lap_count(&self) -> usize {
        self.laps.get()
    }

    pub fn running(&self) -> bool {
        !self.closed.get()
            && self
                .task
                .borrow()
                .as_ref()
                .is_some_and(|handle| !handle.is_finished())
    }

    pub fn abort_pump(&self) {
        if let Some(task) = self.task.borrow().as_ref() {
            task.abort();
        }
    }

    pub fn phase(&self) -> (bool, bool) {
        (self.pumping.get(), self.releasing.get())
    }

    pub fn set_phase(&self, pumping: bool, releasing: bool) {
        self.pumping.set(pumping);
        self.releasing.set(releasing);
    }
}
