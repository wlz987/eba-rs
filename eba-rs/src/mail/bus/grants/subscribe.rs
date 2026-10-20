use std::cell::RefCell;
use std::rc::{Rc, Weak};

use super::super::{Bus, Lease, Subscription};
use crate::envelope::topic::split_topic;
use crate::errors::{BoxError, BusError};
use crate::mail::inbox::Inbox;
use crate::pattern::parse_pattern;

impl Bus {
    pub fn subscribe(
        &self,
        pattern: &str,
        inbox: &Rc<Inbox>,
    ) -> Result<Rc<Subscription>, BoxError> {
        if inbox.is_closed() {
            return Err(Box::from(BusError("inbox closed".into())));
        }
        let parsed = match parse_pattern(pattern) {
            Ok(parsed) => parsed,
            Err(error) => return Err(Box::from(BusError(error.to_string()))),
        };
        let revoke = Rc::clone(&self.revoke);
        let changes = Rc::clone(&self.routes.borrow().changes);
        let added = self
            .routes
            .borrow_mut()
            .subscribe(parsed.clone(), inbox, &revoke);
        if let Some(added) = added {
            changes.notify(&added);
        }
        let routes = Rc::clone(&self.routes);
        let handle = Rc::clone(inbox);
        let holder: Rc<RefCell<Option<Weak<Subscription>>>> = Rc::new(RefCell::new(None));
        let held = Rc::clone(&holder);
        let released = parsed.clone();
        let notified = Rc::clone(&changes);
        let granted = Subscription::new(move || {
            let (found, pending) =
                routes
                    .borrow_mut()
                    .detach(&released, &handle, &revoke, held.borrow().as_ref());
            if let Some(pending) = pending {
                notified.notify(&pending);
            }
            found
        });
        let right = Rc::downgrade(&granted);
        *holder.borrow_mut() = Some(right.clone());
        self.routes.borrow_mut().bind(&parsed, inbox, &right);
        if let Some(frame) = self.grant_frames.borrow_mut().last_mut() {
            let noted: Rc<dyn Lease> = granted.clone();
            frame.push((inbox.id, noted));
        }
        Ok(granted)
    }

    pub fn subscribe_lease(
        &self,
        pattern: &str,
        inbox: &Rc<Inbox>,
    ) -> Result<Rc<dyn Lease>, BoxError> {
        let granted = self.subscribe(pattern, inbox)?;
        let wrapper = self.lease_wrapper.borrow().clone();
        match wrapper {
            Some(wrapper) => match wrapper(Rc::clone(&granted)) {
                Ok(lease) => Ok(lease),
                Err(error) => {
                    granted.close();
                    Err(error)
                }
            },
            None => Ok(granted),
        }
    }

    pub fn subscription_count(&self, topic: &str, inbox: &Rc<Inbox>) -> usize {
        let parts = split_topic(topic).unwrap_or_default();
        self.routes.borrow().hold_count(&parts, inbox)
    }
}
