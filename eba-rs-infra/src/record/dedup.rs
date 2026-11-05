use alloc::collections::BTreeSet;
use alloc::string::String;
use alloc::vec::Vec;

/// Process-local dedup window for at-least-once effects (C1).
pub struct DedupWindow {
    capacity: usize,
    seen: BTreeSet<String>,
    order: Vec<String>,
    head: usize,
}

impl DedupWindow {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "dedup_capacity must be positive");
        Self {
            capacity,
            seen: BTreeSet::new(),
            order: Vec::new(),
            head: 0,
        }
    }

    pub fn size(&self) -> usize {
        self.seen.len()
    }

    pub fn has(&self, key: &str) -> bool {
        self.seen.contains(key)
    }

    /// True if the key is new (and remembers it); false if already seen.
    pub fn accept(&mut self, key: &str) -> bool {
        if self.seen.contains(key) {
            return false;
        }
        self.seen.insert(key.into());
        self.order.push(key.into());
        while self.seen.len() > self.capacity {
            let evicted = self.order[self.head].clone();
            self.head += 1;
            self.seen.remove(&evicted);
        }
        if self.head > 1024 && self.head * 2 > self.order.len() {
            self.order.drain(0..self.head);
            self.head = 0;
        }
        true
    }

    pub fn clear(&mut self) {
        self.seen.clear();
        self.order.clear();
        self.head = 0;
    }
}

pub fn dedup_key(from: &str, id: &str) -> String {
    let mut key = String::with_capacity(from.len() + id.len() + 1);
    key.push_str(from);
    key.push('\0');
    key.push_str(id);
    key
}
