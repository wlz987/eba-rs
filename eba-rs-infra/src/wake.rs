use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use tokio::sync::Notify;

pub struct Wake {
    pending: Cell<bool>,
    notify: Notify,
    waiting: Cell<bool>,
}

impl Wake {
    pub fn new() -> Rc<Wake> {
        Rc::new(Wake {
            pending: Cell::new(false),
            notify: Notify::new(),
            waiting: Cell::new(false),
        })
    }

    pub fn notify(&self) {
        self.pending.set(true);
        self.notify.notify_waiters();
    }

    pub fn consume(&self) -> bool {
        let fired = self.pending.get();
        self.pending.set(false);
        fired
    }

    pub fn waiting(&self) -> bool {
        self.waiting.get()
    }

    pub async fn wait(&self, timeout_ms: Option<u64>) -> bool {
        if self.waiting.get() {
            panic!("Wake has one scheduler waiter");
        }
        if self.pending.get() {
            self.pending.set(false);
            return true;
        }
        if timeout_ms == Some(0) {
            return false;
        }
        self.waiting.set(true);
        let result = match timeout_ms {
            None => {
                self.notify.notified().await;
                true
            }
            Some(ms) => {
                tokio::select! {
                    _ = self.notify.notified() => true,
                    _ = tokio::time::sleep(Duration::from_millis(ms)) => false,
                }
            }
        };
        self.waiting.set(false);
        if result {
            self.pending.set(false);
        }
        result
    }
}
