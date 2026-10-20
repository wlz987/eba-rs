use std::rc::Rc;

use crate::errors::{is_settled, BoxError, DeliveryError};
use crate::job::caller::{Call, Outcome};

use super::Drive;

impl Drive {
    pub(super) async fn run_loop(&self) {
        while !self.closed.get() {
            if let Err(error) = self.release() {
                self.fail(error);
                return;
            }
            self.pumping.set(true);
            self.laps.set(self.laps.get() + 1);
            let outcome = {
                let mut runtime = self.runtime.borrow_mut();
                runtime.advance()
            };
            self.pumping.set(false);
            let moved = match outcome {
                Ok(value) => value,
                Err(error) => {
                    let retired = error.downcast_ref::<DeliveryError>().is_some();
                    if !retired && !is_settled(&error) {
                        self.fail(error);
                        return;
                    }
                    if retired {
                        self.drain();
                    }
                    true
                }
            };
            self.drain();
            if self.closed.get() {
                return;
            }
            let mut delay = self.delay();
            if delay == Some(0) && moved {
                tokio::task::yield_now().await;
                continue;
            }
            if delay == Some(0) {
                delay = None;
            }
            if self.consume_wake() {
                continue;
            }
            self.pause(delay).await;
        }
        if let Err(error) = self.release() {
            self.fail(error);
        }
    }

    fn consume_wake(&self) -> bool {
        self.wake
            .borrow()
            .as_ref()
            .is_some_and(|wake| wake.consume())
    }

    fn fail(&self, error: BoxError) {
        // （见 tests/drive.rs 书头那笔账）。
        let message = error.to_string();
        let mut held = self.fault.borrow_mut();
        if held.is_none() {
            *held = Some(error);
        }
        drop(held);
        self.fail_waiters(&message);
    }

    pub(super) fn fail_waiters(&self, message: &str) {
        let waiters = self.inner.borrow_mut().waiters.take_all();
        for (_, waiter) in waiters {
            let Some(call) = waiter.envelope.payload.downcast_ref::<Call>() else {
                continue;
            };
            if let Some(pay) = call.pay.write().unwrap().take() {
                pay(Outcome::Failed(message.into()));
            }
        }
    }

    fn drain(&self) {
        let retired = std::mem::take(&mut *self.inner.borrow().retired.lock().unwrap());
        if retired.is_empty() {
            return;
        }
        let inner = Rc::clone(&self.inner);
        let mut held = inner.borrow_mut();
        for key in retired {
            held.waiters.remove(&key);
        }
    }

    pub(super) fn delay(&self) -> Option<u64> {
        let mut runtime = self.runtime.borrow_mut();
        let snapshot = runtime.schedule();
        let wait = snapshot.wait_ms(None);
        if wait == Some(0) {
            return Some(0);
        }
        if wait.is_none() || !runtime.clock_ref().arms_timer() {
            return None;
        }
        wait
    }

    async fn pause(&self, delay_ms: Option<u64>) {
        let wake = self.wake.borrow().clone();
        let Some(wake) = wake else {
            return;
        };
        let _ = wake.wait(delay_ms).await;
    }
}
