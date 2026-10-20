use std::collections::VecDeque;

pub struct Queue<T> {
    capacity: usize,
    items: VecDeque<T>,
}

impl<T> Queue<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity >= 1, "capacity must be a positive integer");
        Queue {
            capacity,
            items: VecDeque::new(),
        }
    }

    pub fn size(&self) -> usize {
        self.items.len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn full(&self) -> bool {
        self.items.len() >= self.capacity
    }

    pub fn push(&mut self, item: T) -> bool {
        if self.full() {
            return false;
        }
        self.items.push_back(item);
        true
    }

    pub fn shift(&mut self) -> Option<T> {
        self.items.pop_front()
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.items.iter()
    }
}
