use super::Runtime;
use alloc::string::String;
use alloc::vec::Vec;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HostStats {
    pub active: usize,
    pub held: usize,
    pub queued: usize,
    pub pending: usize,
    pub rejected: usize,
    pub pending_cancels: usize,
    pub mailbox: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RuntimeStats {
    pub hosts: usize,
    pub active: usize,
    pub held: usize,
    pub queued: usize,
    pub pending: usize,
    pub rejected: usize,
    pub pending_cancels: usize,
    pub mailbox: usize,
    pub per_host: Vec<(String, HostStats)>,
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
            let detail = HostStats {
                active: core.jobs.len(),
                held: core
                    .jobs
                    .values()
                    .filter(|job| job.state.borrow().held)
                    .count(),
                queued: core.queue.size(),
                pending: core.book.size(),
                rejected: core.rejected_count,
                pending_cancels: core.cancel_pending.len(),
                mailbox: core.mailbox.size(),
            };
            stats.active += detail.active;
            stats.held += detail.held;
            stats.queued += detail.queued;
            stats.pending += detail.pending;
            stats.rejected += detail.rejected;
            stats.pending_cancels += detail.pending_cancels;
            stats.mailbox += detail.mailbox;
            stats.per_host.push((core.sub.actor_id.0.clone(), detail));
        }
        stats
    }
}
