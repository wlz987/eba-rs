use alloc::vec::Vec;

use crate::job::JobLease;

struct Parked<O> {
    lease: JobLease,
    owner: Option<O>,
    deadline: Option<u64>,
}

/// Deadline/owner index for parked leases. Park owns the key queue; this owns lifecycle.
pub struct ParkedLeases<O> {
    parked: Vec<Parked<O>>,
}

impl<O> Default for ParkedLeases<O> {
    fn default() -> Self {
        Self { parked: Vec::new() }
    }
}

impl<O: PartialEq> ParkedLeases<O> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn park(&mut self, lease: JobLease, owner: Option<O>, deadline: Option<u64>) {
        self.parked.push(Parked {
            lease,
            owner,
            deadline,
        });
    }

    pub fn unpark(&mut self, lease: &JobLease) {
        self.parked.retain(|entry| !entry.lease.same(lease));
    }

    pub fn expire(&mut self, now_ms: u64) -> Vec<JobLease> {
        let mut expired = Vec::new();
        self.parked.retain(|entry| {
            let due = entry.deadline.is_some_and(|deadline| deadline <= now_ms);
            if due && !entry.lease.is_closed() {
                expired.push(entry.lease.clone());
            }
            !due
        });
        expired
    }

    pub fn cancel_owner(&mut self, owner: &O) -> Vec<JobLease> {
        let mut cancelled = Vec::new();
        self.parked.retain(|entry| {
            let mine = entry.owner.as_ref() == Some(owner);
            if mine && !entry.lease.is_closed() {
                cancelled.push(entry.lease.clone());
            }
            !mine
        });
        cancelled
    }
}
