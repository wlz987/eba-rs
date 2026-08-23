use std::rc::Rc;
use std::time::Instant;

pub trait Clock {
    fn now_ms(&self) -> i64;
}

pub type ClockHandle = Rc<dyn Clock>;

#[derive(Debug)]
pub struct MonotonicClock {
    start: Instant,
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl MonotonicClock {
    pub fn new() -> MonotonicClock {
        MonotonicClock {
            start: Instant::now(),
        }
    }
}

impl Clock for MonotonicClock {
    fn now_ms(&self) -> i64 {
        self.start.elapsed().as_millis() as i64
    }
}

#[derive(Debug, Default)]
pub struct ManualClock {
    now: std::cell::Cell<i64>,
}

impl ManualClock {
    pub fn new(now: i64) -> ManualClock {
        ManualClock {
            now: std::cell::Cell::new(now),
        }
    }

    pub fn advance(&self, ms: i64) {
        self.now.set(self.now.get() + ms);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> i64 {
        self.now.get()
    }
}
