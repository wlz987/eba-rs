use std::cell::Cell;

pub trait Clock {
    fn now_ms(&self) -> u64;
    fn arms_timer(&self) -> bool {
        true
    }
}

pub struct ManualClock {
    now: Cell<u64>,
}

impl ManualClock {
    pub fn new(now_ms: u64) -> Self {
        ManualClock {
            now: Cell::new(now_ms),
        }
    }

    pub fn advance(&self, ms: u64) {
        self.now.set(self.now.get() + ms);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.now.get()
    }

    fn arms_timer(&self) -> bool {
        false
    }
}

pub struct MonotonicClock;

static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();

impl Clock for MonotonicClock {
    fn now_ms(&self) -> u64 {
        let start = START.get_or_init(std::time::Instant::now);
        start.elapsed().as_millis() as u64
    }
}

pub struct PinnedClock {
    instant: u64,
}

impl PinnedClock {
    pub fn new(now_ms: u64) -> Self {
        PinnedClock { instant: now_ms }
    }
}

impl Clock for PinnedClock {
    fn now_ms(&self) -> u64 {
        self.instant
    }
}

pub fn pin_clock(now_ms: u64) -> PinnedClock {
    PinnedClock::new(now_ms)
}
