use crate::prelude::*;
use alloc::collections::{BTreeMap, VecDeque};

use crate::job::{JobLease, ParkedLeases};

/// 每个键最多保留的唤醒信用；满了丢新的。
const CREDIT_LIMIT: usize = 4096;

struct Waiter {
    lease: JobLease,
}

fn pop_waiter<O: PartialEq>(
    queue: &mut VecDeque<Waiter>,
    registry: &mut ParkedLeases<O>,
) -> Option<Waiter> {
    while let Some(waiter) = queue.pop_front() {
        if !waiter.lease.is_closed() {
            return Some(waiter);
        }
        registry.unpark(&waiter.lease);
    }
    None
}

/// Keyed waits: many JobLeases per key, one wake consumed by one live waiter.
/// A wake that finds nobody is kept as a credit; a key keeps at most
/// 4096 credits. Deadlines and owners live in LeaseRegistry; Park only owns the
/// key index and credits.
pub struct Park<K: Ord + Clone, V, O: Ord + Clone = ()> {
    waiters: BTreeMap<K, VecDeque<Waiter>>,
    credits: BTreeMap<K, VecDeque<V>>,
    registry: ParkedLeases<O>,
}

impl<K: Ord + Clone, V, O: Ord + Clone> Default for Park<K, V, O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Ord + Clone, V, O: Ord + Clone> Park<K, V, O> {
    pub fn new() -> Self {
        Park {
            waiters: BTreeMap::new(),
            credits: BTreeMap::new(),
            registry: ParkedLeases::new(),
        }
    }

    /// Consume a pending credit (leaving `lease` to the caller), otherwise park it.
    /// `deadline` is absolute ms; `owner` scopes cancellation.
    pub fn wait(
        &mut self,
        key: K,
        lease: JobLease,
        deadline: Option<u64>,
        owner: Option<O>,
    ) -> Option<V> {
        if lease.is_closed() {
            return None;
        }
        let mut ready = None;
        if let Some(credits) = self.credits.get_mut(&key) {
            ready = credits.pop_front();
        }
        if self
            .credits
            .get(&key)
            .is_some_and(|credits| credits.is_empty())
        {
            self.credits.remove(&key);
        }
        if let Some(value) = ready {
            return Some(value);
        }
        self.registry.park(lease.clone(), owner, deadline);
        self.waiters
            .entry(key)
            .or_default()
            .push_back(Waiter { lease });
        None
    }

    /// Wake the oldest live waiter, or keep `value` as a credit.
    pub fn wake(&mut self, key: &K, value: V) -> Option<JobLease> {
        let waiters = &mut self.waiters;
        let registry = &mut self.registry;
        let waiter = waiters
            .get_mut(key)
            .and_then(|queue| pop_waiter(queue, registry));
        self.prune(key);
        if let Some(waiter) = waiter {
            self.registry.unpark(&waiter.lease);
            return Some(waiter.lease);
        }
        let credits = self.credits.entry(key.clone()).or_default();
        if credits.len() < CREDIT_LIMIT {
            credits.push_back(value);
        }
        None
    }

    /// Take up to `count` live waiters without leaving credits.
    pub fn take(&mut self, key: &K, count: usize) -> Vec<JobLease> {
        if count == 0 {
            return Vec::new();
        }
        let mut taken = Vec::new();
        let waiters = &mut self.waiters;
        let registry = &mut self.registry;
        if let Some(queue) = waiters.get_mut(key) {
            while taken.len() < count {
                let Some(waiter) = pop_waiter(queue, registry) else {
                    break;
                };
                registry.unpark(&waiter.lease);
                taken.push(waiter.lease);
            }
        }
        self.prune(key);
        taken
    }

    /// Move up to `count` live waiters from `from` to `to`; keys must differ.
    pub fn move_waiters(&mut self, from: &K, to: K, count: usize) -> usize {
        if from == &to || count == 0 {
            return 0;
        }
        let Some(mut source) = self.waiters.remove(from) else {
            return 0;
        };
        let mut moved = 0;
        {
            let registry = &mut self.registry;
            let target = self.waiters.entry(to.clone()).or_default();
            while moved < count {
                let Some(waiter) = pop_waiter(&mut source, registry) else {
                    break;
                };
                target.push_back(waiter);
                moved += 1;
            }
        }
        if !source.is_empty() {
            self.waiters.insert(from.clone(), source);
        }
        self.prune(&to);
        moved
    }

    /// Wake up to `wake` waiters, then move up to `requeue` waiters to `to`.
    /// A same-key requeue only wakes; it does not reorder.
    pub fn requeue(&mut self, from: &K, to: K, wake: usize, requeue: usize) -> Vec<JobLease> {
        let woken = self.take(from, wake);
        self.move_waiters(from, to, requeue);
        woken
    }

    /// Collect waiters whose deadline passed; lifecycle comes from LeaseRegistry.
    pub fn expire(&mut self, now_ms: u64) -> Vec<JobLease> {
        let expired = self.registry.expire(now_ms);
        self.remove(&expired);
        expired
    }

    /// Drop a key's waiters and credits, returning the live waiters.
    pub fn cancel(&mut self, key: &K) -> Vec<JobLease> {
        self.credits.remove(key);
        let Some(queue) = self.waiters.remove(key) else {
            return Vec::new();
        };
        let mut cancelled = Vec::new();
        for waiter in queue {
            self.registry.unpark(&waiter.lease);
            if !waiter.lease.is_closed() {
                cancelled.push(waiter.lease);
            }
        }
        cancelled
    }

    /// Drop one owner's waiters across keys; credits are key-scoped and stay.
    pub fn cancel_owner(&mut self, owner: &O) -> Vec<JobLease> {
        let cancelled = self.registry.cancel_owner(owner);
        self.remove(&cancelled);
        cancelled
    }

    /// Keys with at least one waiter; credit-only keys are not listed.
    pub fn keys(&self) -> Vec<K> {
        self.waiters.keys().cloned().collect()
    }

    fn remove(&mut self, leases: &[JobLease]) {
        if leases.is_empty() {
            return;
        }
        for queue in self.waiters.values_mut() {
            queue.retain(|waiter| !leases.iter().any(|lease| lease.same(&waiter.lease)));
        }
        let keys: Vec<K> = self.waiters.keys().cloned().collect();
        for key in keys {
            self.prune(&key);
        }
    }

    fn prune(&mut self, key: &K) {
        if self.waiters.get(key).is_some_and(|queue| queue.is_empty()) {
            self.waiters.remove(key);
        }
    }
}
