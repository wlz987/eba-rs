use std::collections::HashMap;
use std::hash::Hash;

pub const ELAPSED: u64 = 0;

pub struct DeadlinesLedger<K> {
    pub items: Vec<(u64, u64, K)>,
    pub positions: Vec<(K, usize)>,
    pub sequence: u64,
}
pub struct Deadlines<K> {
    items: Vec<(u64, u64, K)>,
    positions: HashMap<K, usize>,
    sequence: u64,
}

impl<K: Clone + Eq + Hash> Deadlines<K> {
    pub fn new() -> Self {
        Deadlines {
            items: Vec::new(),
            positions: HashMap::new(),
            sequence: 0,
        }
    }

    pub fn at(&self, item: &K) -> Option<u64> {
        self.positions.get(item).map(|i| self.items[*i].0)
    }

    pub fn timed(&self) -> Vec<(K, u64)> {
        self.items
            .iter()
            .map(|(at, _, item)| (item.clone(), *at))
            .collect()
    }

    pub fn next_ms(&self) -> Option<u64> {
        self.items.first().map(|item| item.0)
    }

    pub fn add(&mut self, item: K, at: u64) {
        if self.positions.contains_key(&item) {
            panic!("duplicate deadline");
        }
        self.sequence += 1;
        let index = self.items.len();
        self.positions.insert(item.clone(), index);
        self.items.push((at, self.sequence, item));
        self.up(index);
    }

    pub fn remove(&mut self, item: &K) {
        let Some(index) = self.positions.remove(item) else {
            return;
        };
        let last = self.items.pop().unwrap();
        if index == self.items.len() {
            return;
        }
        self.items[index] = last;
        let key = self.items[index].2.clone();
        self.positions.insert(key, index);
        self.up(index);
        self.down(index);
    }

    pub fn peek_due(&self, now_ms: u64) -> Option<&K> {
        match self.items.first() {
            Some(item) if item.0 <= now_ms => Some(&item.2),
            _ => None,
        }
    }

    pub fn inspect(&self) -> DeadlinesLedger<K> {
        DeadlinesLedger {
            items: self.items.clone(),
            positions: self
                .positions
                .iter()
                .map(|(item, index)| (item.clone(), *index))
                .collect(),
            sequence: self.sequence,
        }
    }

    fn before(&self, left: &(u64, u64, K), right: &(u64, u64, K)) -> bool {
        left.0 < right.0 || (left.0 == right.0 && left.1 < right.1)
    }

    fn swap(&mut self, left: usize, right: usize) {
        let a = self.items[left].2.clone();
        let b = self.items[right].2.clone();
        self.items.swap(left, right);
        self.positions.insert(a, right);
        self.positions.insert(b, left);
    }

    fn up(&mut self, mut index: usize) {
        while index > 0 {
            let parent = (index - 1) / 2;
            if !self.before(&self.items[index], &self.items[parent]) {
                return;
            }
            self.swap(index, parent);
            index = parent;
        }
    }

    fn down(&mut self, mut index: usize) {
        let length = self.items.len();
        while 2 * index + 1 < length {
            let mut child = 2 * index + 1;
            if child + 1 < length && self.before(&self.items[child + 1], &self.items[child]) {
                child += 1;
            }
            if !self.before(&self.items[child], &self.items[index]) {
                return;
            }
            self.swap(index, child);
            index = child;
        }
    }
}

impl<K: Clone + Eq + Hash> Default for Deadlines<K> {
    fn default() -> Self {
        Self::new()
    }
}
