use crate::prelude::*;
use alloc::rc::Rc;
use core::any::Any;
use core::cell::RefCell;

use crate::errors::BoxError;

pub trait Lease {
    fn close(&self) -> Result<(), BoxError>;
    fn subscription_ptr(&self) -> Option<*const Subscription>;
    fn active(&self) -> Option<bool> {
        None
    }
    fn as_any(&self) -> &dyn Any;
}

impl Lease for Subscription {
    fn close(&self) -> Result<(), BoxError> {
        Subscription::close(self);
        Ok(())
    }

    fn subscription_ptr(&self) -> Option<*const Subscription> {
        Some(self as *const Subscription)
    }

    fn active(&self) -> Option<bool> {
        Some(Subscription::active(self))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct Subscription {
    release: RefCell<Option<Box<dyn Fn() -> bool>>>,
}

impl Subscription {
    pub(super) fn new(release: impl Fn() -> bool + 'static) -> Rc<Subscription> {
        Rc::new(Subscription {
            release: RefCell::new(Some(Box::new(release))),
        })
    }

    pub fn close(&self) -> bool {
        let release = self.release.borrow_mut().take();
        match release {
            Some(release) => release(),
            None => false,
        }
    }

    pub fn disarm(&self) {
        *self.release.borrow_mut() = None;
    }

    pub fn active(&self) -> bool {
        self.release.borrow().is_some()
    }
}

pub type LeaseWrapper = Rc<dyn Fn(Rc<Subscription>) -> Result<Rc<dyn Lease>, BoxError>>;
