use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};

pub const READ: u32 = 1;
pub const TIMER: u32 = 2;
pub const WORK: u32 = 4;
pub const DELIVERY: u32 = 8;
const MASK: u32 = 0xffff_ffff;

pub trait AgendaHost {
    fn enqueue(&self, actor: &str);
    fn unqueue(&self, actor: &str);
    fn bind_signal(&self, actor: &str, signal: Weak<Signal>);
    fn has_signal(&self, actor: &str) -> bool;
}

pub struct Signal {
    flags: Cell<u32>,
    owner: RefCell<Option<Weak<dyn AgendaHost>>>,
    id: RefCell<Option<String>>,
}

impl Signal {
    pub fn new() -> Self {
        Signal {
            flags: Cell::new(0),
            owner: RefCell::new(None),
            id: RefCell::new(None),
        }
    }

    pub fn handed(&self) -> bool {
        self.owner.borrow().is_some()
    }

    pub fn is_bound(&self) -> bool {
        self.owner
            .borrow()
            .as_ref()
            .and_then(|w| w.upgrade())
            .is_some()
    }

    pub fn bound_to(&self, owner: &Rc<dyn AgendaHost>) -> bool {
        self.owner
            .borrow()
            .as_ref()
            .and_then(|w| w.upgrade())
            .is_some_and(|held| Rc::ptr_eq(&held, owner))
    }

    pub fn has(&self, flag: u32) -> bool {
        (self.flags.get() & flag) != 0
    }

    pub fn notify(&self, flag: u32) {
        self.flags.set(self.flags.get() | flag);
        let id = self.id.borrow().clone();
        let owner = self.owner.borrow().as_ref().and_then(|w| w.upgrade());
        if let (Some(id), Some(owner)) = (id, owner) {
            owner.enqueue(&id);
        }
    }

    pub fn clear(&self, flag: u32) {
        self.flags.set(self.flags.get() & !flag & MASK);
        if self.flags.get() != 0 {
            return;
        }
        let id = self.id.borrow().clone();
        let owner = self.owner.borrow().as_ref().and_then(|w| w.upgrade());
        if let (Some(id), Some(owner)) = (id, owner) {
            owner.unqueue(&id);
        }
    }

    pub fn set(&self, flag: u32, ready: bool) {
        if ready {
            self.notify(flag);
        } else {
            self.clear(flag);
        }
    }

    pub fn bind(self: &Rc<Signal>, owner: &Rc<dyn AgendaHost>, actor: &str) {
        *self.owner.borrow_mut() = Some(Rc::downgrade(owner));
        *self.id.borrow_mut() = Some(actor.to_string());
        owner.bind_signal(actor, Rc::downgrade(self));
        if self.flags.get() != 0 {
            owner.enqueue(actor);
        }
    }

    pub fn detach(&self) {
        *self.owner.borrow_mut() = None;
        *self.id.borrow_mut() = None;
    }
}

impl Default for Signal {
    fn default() -> Self {
        Self::new()
    }
}
