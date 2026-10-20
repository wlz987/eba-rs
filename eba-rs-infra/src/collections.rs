use crate::prelude::*;
#[derive(Debug, Clone)]
pub struct IndexedMap<K, V> {
    items: Vec<(K, V)>,
}

impl<K, V> Default for IndexedMap<K, V> {
    fn default() -> Self {
        IndexedMap { items: Vec::new() }
    }
}

impl<K: PartialEq, V> IndexedMap<K, V> {
    pub fn new() -> Self {
        Self::default()
    }

    fn index_of(&self, key: &K) -> Option<usize> {
        self.items.iter().position(|(at, _)| at == key)
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.index_of(key).is_some()
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.index_of(key).map(|index| &self.items[index].1)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.index_of(key)
            .map(move |index| &mut self.items[index].1)
    }

    pub fn insert(&mut self, key: K, value: V) {
        match self.index_of(&key) {
            Some(index) => self.items[index].1 = value,
            None => self.items.push((key, value)),
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let index = self.index_of(key)?;
        Some(self.items.remove(index).1)
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&K) -> bool) {
        self.items.retain(|(key, _)| keep(key));
    }

    pub fn clear(&mut self) {
        self.items.clear();
    }

    pub fn take_all(&mut self) -> Vec<(K, V)> {
        core::mem::take(&mut self.items)
    }

    pub fn iter(&self) -> impl Iterator<Item = &(K, V)> {
        self.items.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&mut K, &mut V)> {
        self.items.iter_mut().map(|(key, value)| (key, value))
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.items.iter().map(|(key, _)| key)
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.items.iter().map(|(_, value)| value)
    }

    pub fn first_key(&self) -> Option<&K> {
        self.items.first().map(|(key, _)| key)
    }
}
