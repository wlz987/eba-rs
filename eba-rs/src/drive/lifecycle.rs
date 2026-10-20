use std::rc::Rc;

use crate::errors::BoxError;

use super::Drive;

impl Drive {
    pub fn start(self: &Rc<Self>) {
        if self
            .task
            .borrow()
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
        {
            panic!("drive already running");
        }
        self.reset();
        let runtime = self.runtime.borrow();
        let weak = Rc::downgrade(self);
        runtime.bind_wake(Some(Box::new(move || {
            if let Some(this) = weak.upgrade() {
                this.kick();
            }
            Ok(())
        })));
        let this = Rc::clone(self);
        let handle = tokio::task::spawn_local(async move {
            this.run_loop().await;
        });
        *self.task.borrow_mut() = Some(handle);
    }

    pub async fn close(&self) -> Result<(), BoxError> {
        self.closed.set(true);
        self.kick();
        let task = self.task.borrow_mut().take();
        if let Some(task) = task {
            let _ = task.await;
        }
        self.runtime.borrow().bind_wake(None);
        self.fail_waiters("drive closed");
        let mut held = self.fault.borrow_mut();
        match held.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub fn kick(&self) {
        if self.pumping.get() || self.releasing.get() {
            return;
        }
        if let Some(wake) = self.wake.borrow().clone() {
            wake.notify();
        }
    }
}
