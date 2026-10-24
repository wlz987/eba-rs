use super::Runtime;
use alloc::string::String;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RuntimeStats {
    pub hosts: usize,
    pub active: usize,
    pub held: usize,
    pub queued: usize,
    pub pending: usize,
    pub rejected: usize,
    pub cancel_errors: usize,
    pub last_cancel_error: Option<String>,
    pub pending_cancels: usize,
}

impl Runtime {
    pub fn stats(&self) -> RuntimeStats {
        let mut stats = RuntimeStats {
            hosts: self.hosts.len(),
            ..RuntimeStats::default()
        };
        for host in self.hosts.values() {
            let core = host.core();
            let core = core.borrow();
            stats.active += core.jobs.len();
            stats.held += core
                .jobs
                .values()
                .filter(|job| job.state.borrow().held)
                .count();
            stats.queued += core.queue.size();
            stats.pending += core.book.size();
            stats.rejected += core.rejected_count;
            stats.cancel_errors += core.cancel_errors;
            stats.pending_cancels += core.cancel_pending.len();
            if core.last_cancel_error.is_some() {
                stats.last_cancel_error = core.last_cancel_error.clone();
            }
        }
        stats
    }
}
