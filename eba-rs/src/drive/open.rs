use std::rc::Rc;

use crate::envelope::Envelope;
use crate::errors::{BoxError, BusError};
use crate::job::caller::{Call, CallOptions, Caller, Outcome};
use crate::job::host::JobHost;
use tokio::sync::oneshot;

use super::{Drive, Pending, Waiter};

impl Drive {
    pub fn open(&self, caller: Rc<Caller>, options: CallOptions) -> Result<Pending, BoxError> {
        self.require_running()?;
        {
            let mut runtime = self.runtime.borrow_mut();
            let core = caller.core();
            if core.borrow().signal.handed() {
                if !runtime.owns(&*caller) {
                    return Err(Box::from(BusError(
                        "caller belongs to another runtime".into(),
                    )));
                }
            } else {
                runtime.add(Rc::clone(&caller) as Rc<dyn JobHost>)?;
            }
        }
        let envelope = caller.admit(options)?;
        let key = envelope.header.id.0.clone();
        let Some(call) = envelope.payload.downcast_ref::<Call>() else {
            return Err(Box::from(BusError("admitted root has no call".into())));
        };
        let (tx, rx) = oneshot::channel();
        self.inner.borrow_mut().waiters.insert(
            key.clone(),
            Waiter {
                envelope: Rc::clone(&envelope),
            },
        );
        let retired = Rc::clone(&self.inner).borrow().retired.clone();
        call.set_pay(Box::new(move |outcome| match outcome {
            Outcome::Settled(completion) => {
                let _ = tx.send(Ok(completion));
                retired.lock().unwrap().push(key);
            }
            Outcome::Failed(message) => {
                let _ = tx.send(Err(Box::from(BusError(message)) as BoxError));
            }
        }));
        Ok(Pending {
            envelope,
            ready: rx,
        })
    }

    /// Give up waiting on one root; the pump cancels it as soon as the host allows.
    pub fn abandon(&self, caller: Rc<Caller>, envelope: &Rc<Envelope>) {
        let key = envelope.header.id.0.clone();
        if self.inner.borrow_mut().waiters.remove(&key).is_none() {
            return;
        }
        let Some(call) = envelope.payload.downcast_ref::<Call>() else {
            return;
        };
        if call.pay.write().unwrap().take().is_none() {
            return;
        }
        self.inner
            .borrow_mut()
            .stops
            .push((caller, envelope.header.id.0.clone()));
        self.kick();
    }

    fn require_running(&self) -> Result<(), BoxError> {
        let stopped = match &*self.task.borrow() {
            None => true,
            Some(handle) => handle.is_finished(),
        };
        if self.closed.get() || stopped {
            return Err(Box::from(BusError("drive is not running".into())));
        }
        Ok(())
    }
}
