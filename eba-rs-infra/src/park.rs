use crate::prelude::*;
use alloc::collections::{BTreeMap, VecDeque};

use crate::job::Reply;

/// 每个键最多保留的唤醒信用；满了丢新的。
const CREDIT_LIMIT: usize = 4096;

struct Waiter<O> {
    reply: Reply,
    deadline: Option<u64>,
    owner: Option<O>,
}

fn pop_waiter<O>(queue: &mut VecDeque<Waiter<O>>) -> Option<Waiter<O>> {
    while let Some(waiter) = queue.pop_front() {
        if !waiter.reply.is_closed() {
            return Some(waiter);
        }
    }
    None
}

/// Keyed waits: many Replies per key, one wake consumed by one live waiter.
/// A wake that finds nobody is kept as a credit; a key keeps at most
/// 4096 credits. Wakes, expiries and cancellations return live Replies for the
/// caller to settle; a consumed credit leaves its Reply to the caller. An owner
/// scopes cancellation: `cancel_owner` never touches another owner's wait.
pub struct Park<K: Ord + Clone, V, O: Ord + Clone = ()> {
    waiters: BTreeMap<K, VecDeque<Waiter<O>>>,
    credits: BTreeMap<K, VecDeque<V>>,
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
        }
    }

    /// Consume a pending credit (leaving `reply` to the caller), otherwise park it.
    /// `deadline` is an absolute millisecond mark; `owner` scopes cancellation.
    pub fn wait(
        &mut self,
        key: K,
        reply: Reply,
        deadline: Option<u64>,
        owner: Option<O>,
    ) -> Option<V> {
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
        self.waiters.entry(key).or_default().push_back(Waiter {
            reply,
            deadline,
            owner,
        });
        None
    }

    /// Wake the oldest live waiter, or keep `value` as a credit.
    pub fn wake(&mut self, key: &K, value: V) -> Option<Reply> {
        let waiter = self.waiters.get_mut(key).and_then(pop_waiter);
        self.prune(key);
        if let Some(waiter) = waiter {
            return Some(waiter.reply);
        }
        let credits = self.credits.entry(key.clone()).or_default();
        if credits.len() < CREDIT_LIMIT {
            credits.push_back(value);
        }
        None
    }

    /// Take up to `count` live waiters without leaving credits.
    pub fn take(&mut self, key: &K, count: usize) -> Vec<Reply> {
        if count == 0 {
            return Vec::new();
        }
        let mut taken = Vec::new();
        if let Some(queue) = self.waiters.get_mut(key) {
            while taken.len() < count {
                let Some(waiter) = pop_waiter(queue) else {
                    break;
                };
                taken.push(waiter.reply);
            }
        }
        self.prune(key);
        taken
    }

    /// Move up to `count` live waiters from `from` to `to`; the keys must differ.
    pub fn move_waiters(&mut self, from: &K, to: K, count: usize) -> usize {
        if from == &to || count == 0 {
            return 0;
        }
        let Some(mut source) = self.waiters.remove(from) else {
            return 0;
        };
        let mut moved = 0;
        {
            let target = self.waiters.entry(to.clone()).or_default();
            while moved < count {
                let Some(waiter) = pop_waiter(&mut source) else {
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
    pub fn requeue(&mut self, from: &K, to: K, wake: usize, requeue: usize) -> Vec<Reply> {
        let woken = self.take(from, wake);
        self.move_waiters(from, to, requeue);
        woken
    }

    /// Collect waiters whose deadline passed; every key is scanned.
    pub fn expire(&mut self, now_ms: u64) -> Vec<Reply> {
        let keys: Vec<K> = self.waiters.keys().cloned().collect();
        let mut expired = Vec::new();
        for key in keys {
            if let Some(queue) = self.waiters.get_mut(&key) {
                queue.retain(|waiter| {
                    if waiter.deadline.is_some_and(|deadline| deadline <= now_ms) {
                        if !waiter.reply.is_closed() {
                            expired.push(waiter.reply.clone());
                        }
                        false
                    } else {
                        true
                    }
                });
            }
            self.prune(&key);
        }
        expired
    }

    /// Drop a key's waiters and credits, returning the live waiters.
    pub fn cancel(&mut self, key: &K) -> Vec<Reply> {
        self.credits.remove(key);
        let Some(queue) = self.waiters.remove(key) else {
            return Vec::new();
        };
        queue
            .into_iter()
            .filter(|waiter| !waiter.reply.is_closed())
            .map(|waiter| waiter.reply)
            .collect()
    }

    /// Drop one owner's waiters across keys; credits are key-scoped and stay.
    pub fn cancel_owner(&mut self, owner: &O) -> Vec<Reply> {
        let keys: Vec<K> = self.waiters.keys().cloned().collect();
        let mut cancelled = Vec::new();
        for key in keys {
            if let Some(queue) = self.waiters.get_mut(&key) {
                queue.retain(|waiter| {
                    if waiter.owner.as_ref() == Some(owner) {
                        if !waiter.reply.is_closed() {
                            cancelled.push(waiter.reply.clone());
                        }
                        false
                    } else {
                        true
                    }
                });
            }
            self.prune(&key);
        }
        cancelled
    }

    /// Keys with at least one waiter; credit-only keys are not listed.
    pub fn keys(&self) -> Vec<K> {
        self.waiters.keys().cloned().collect()
    }

    fn prune(&mut self, key: &K) {
        if self.waiters.get(key).is_some_and(|queue| queue.is_empty()) {
            self.waiters.remove(key);
        }
    }
}
