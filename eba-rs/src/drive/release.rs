use std::cell::RefCell;
use std::rc::Rc;

use crate::collections::IndexedMap;
use crate::envelope::ids::EnvelopeId;
use crate::errors::{BoxError, MailboxFull};
use crate::job::caller::Caller;
use crate::job::host::JobHost;
use crate::mail::inbox::watch_space;
use crate::notify::signal::Signal;
use crate::notify::watch::Interest;

use super::{shut_door, Drive};

impl Drive {
    pub(super) fn release(&self) -> Result<(), BoxError> {
        self.releasing.set(true);
        let outcome = self.release_pass();
        self.releasing.set(false);
        outcome
    }

    fn release_pass(&self) -> Result<(), BoxError> {
        let mut inner = self.inner.borrow_mut();
        let mut kept = Vec::new();
        for (caller, root_id) in inner.stops.clone() {
            match caller.cancel(&EnvelopeId(root_id.clone())) {
                Ok(_) => {}
                Err(error) => {
                    if error.downcast_ref::<MailboxFull>().is_some() {
                        self.arm_space(&caller);
                        kept.push((caller, root_id));
                    } else if shut_door(&caller) {
                    } else {
                        return Err(error);
                    }
                }
            }
        }
        inner.stops = kept;
        Ok(())
    }

    pub fn arm_space(&self, caller: &Rc<Caller>) {
        let core_ref = caller.core();
        let inbox = Rc::clone(&core_ref.borrow().sub.inbox);
        let key = Rc::as_ptr(&inbox) as usize;
        if self.space_kicks.borrow().contains_key(&key) {
            return;
        }
        let interest = Rc::new(Interest::new(
            Rc::new(Signal::new()),
            Rc::new(RefCell::new(IndexedMap::new())),
            String::new(),
        ));
        let weak = self.slf.borrow().clone();
        interest.set_on_notify(Rc::new(move || {
            if let Some(this) = weak.upgrade() {
                this.space_kicks.borrow_mut().remove(&key);
                if let Some(wake) = this.wake.borrow().clone() {
                    wake.notify();
                }
            }
        }));
        self.space_kicks
            .borrow_mut()
            .insert(key, Rc::clone(&interest));
        watch_space(&inbox, &interest);
    }
}
