#[derive(Debug, Clone)]
pub struct Schedule {
    pub ready: bool,
    pub next_deadline_ms: Option<u64>,
    pub is_quiescent: bool,
    pub now_ms: Option<u64>,
}

impl Schedule {
    pub fn new(ready: bool, next_deadline_ms: Option<u64>, is_quiescent: bool) -> Self {
        Schedule {
            ready,
            next_deadline_ms,
            is_quiescent,
            now_ms: None,
        }
    }

    pub fn observed(mut self, now_ms: u64) -> Self {
        self.now_ms = Some(now_ms);
        self
    }

    pub fn merge(&self, other: &Schedule) -> Schedule {
        let deadline = match (self.next_deadline_ms, other.next_deadline_ms) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        Schedule {
            ready: self.ready || other.ready,
            next_deadline_ms: deadline,
            is_quiescent: self.is_quiescent && other.is_quiescent,
            now_ms: None,
        }
    }

    pub fn wait_ms(&self, now_ms: Option<u64>) -> Option<u64> {
        let instant = match now_ms {
            Some(instant) => instant,
            None => self.now_ms.expect("schedule has no observed instant"),
        };
        if self.ready {
            return Some(0);
        }
        self.next_deadline_ms
            .map(|deadline| deadline.saturating_sub(instant))
    }
}
