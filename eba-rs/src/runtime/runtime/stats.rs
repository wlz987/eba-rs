use super::Runtime;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HostStats {
    pub active: usize,
    pub held: usize,
    pub queued: usize,
    pub pending: usize,
    pub rejected: usize,
    pub cancel_errors: usize,
    pub pending_cancels: usize,
    pub outbox: usize,
    pub buried: usize,
    pub last_cancel_error: Option<String>,
}

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
    pub outbox: usize,
    pub buried: usize,
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
                cancel_errors: core.cancel_errors,
                pending_cancels: core.cancel_pending.len(),
                outbox: core.outbox.size(),
                buried: core.outbox.buried(),
                last_cancel_error: core.last_cancel_error.clone(),
            };
            stats.active += detail.active;
            stats.held += detail.held;
            stats.queued += detail.queued;
            stats.pending += detail.pending;
            stats.rejected += detail.rejected;
            stats.cancel_errors += detail.cancel_errors;
            stats.pending_cancels += detail.pending_cancels;
            stats.outbox += detail.outbox;
            stats.buried += detail.buried;
            if detail.last_cancel_error.is_some() {
                stats.last_cancel_error = detail.last_cancel_error.clone();
            }
            stats.per_host.push((core.sub.actor_id.0.clone(), detail));
        }
        stats
    }
}
