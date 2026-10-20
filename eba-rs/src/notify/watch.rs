use crate::collections::IndexedMap;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::{Rc, Weak};

use super::signal::{Signal, DELIVERY};
use crate::pattern::{lookup_keys, Pattern, Wildcard};

pub struct Interest {
    pub signal: Rc<Signal>,
    pub candidates: Rc<RefCell<IndexedMap<String, ()>>>,
    pub topic: String,
    owners: RefCell<Vec<Weak<Watchers>>>,
    on_notify: RefCell<Option<Rc<dyn Fn()>>>,
}

impl Interest {
    pub fn new(
        signal: Rc<Signal>,
        candidates: Rc<RefCell<IndexedMap<String, ()>>>,
        topic: String,
    ) -> Self {
        Interest {
            signal,
            candidates,
            topic,
            owners: RefCell::new(Vec::new()),
            on_notify: RefCell::new(None),
        }
    }

    pub fn owner_count(&self) -> usize {
        self.owners.borrow().len()
    }

    pub fn set_on_notify(&self, hook: Rc<dyn Fn()>) {
        *self.on_notify.borrow_mut() = Some(hook);
    }

    pub fn notify(&self) {
        let hook = self.on_notify.borrow().clone();
        if let Some(hook) = hook {
            hook();
            return;
        }
        self.candidates.borrow_mut().insert(self.topic.clone(), ());
        self.signal.notify(DELIVERY);
    }

    pub fn dispose(&self) {
        let owners: Vec<_> = self.owners.borrow().iter().cloned().collect();
        for owner in owners.iter() {
            if let Some(owner) = owner.upgrade() {
                owner.remove(self);
            }
        }
        self.owners.borrow_mut().clear();
    }
}

pub type WatcherRc = Rc<Watchers>;

pub struct Watchers {
    waiting: RefCell<Vec<Weak<Interest>>>,
    on_empty: RefCell<Option<Box<dyn Fn()>>>,
}

impl Watchers {
    pub fn new() -> Self {
        Watchers {
            waiting: RefCell::new(Vec::new()),
            on_empty: RefCell::new(None),
        }
    }

    pub fn with_on_empty(on_empty: Box<dyn Fn()>) -> Self {
        Watchers {
            waiting: RefCell::new(Vec::new()),
            on_empty: RefCell::new(Some(on_empty)),
        }
    }

    pub fn watch(self: &Rc<Self>, interest: &Rc<Interest>) {
        let already = self
            .waiting
            .borrow()
            .iter()
            .any(|held| held.as_ptr() == Rc::as_ptr(interest));
        if already {
            return;
        }
        interest.owners.borrow_mut().push(Rc::downgrade(self));
        self.waiting.borrow_mut().push(Rc::downgrade(interest));
    }

    pub fn live(&self) -> Vec<Rc<Interest>> {
        self.waiting
            .borrow()
            .iter()
            .filter_map(Weak::upgrade)
            .collect()
    }

    pub fn remove(&self, interest: &Interest) {
        let gone = interest as *const Interest;
        let mut waiting = self.waiting.borrow_mut();
        waiting.retain(|item| item.as_ptr() != gone);
        if waiting.is_empty() {
            let on_empty = self.on_empty.borrow();
            if let Some(cb) = &*on_empty {
                cb();
            }
        }
    }

    pub fn notify(self: &Rc<Self>) {
        let waiting = self.waiting.borrow().clone();
        self.waiting.borrow_mut().clear();
        let on_empty = self.on_empty.borrow();
        if let Some(cb) = &*on_empty {
            cb();
        }
        drop(on_empty);
        for weak in waiting {
            let Some(interest) = weak.upgrade() else {
                continue;
            };
            interest.owners.borrow_mut().retain(|w| {
                w.upgrade()
                    .is_some_and(|watcher| !Rc::ptr_eq(&watcher, self))
            });
            interest.notify();
        }
    }
}

impl Default for Watchers {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Changes {
    indexes: Rc<RefCell<HashMap<Wildcard, HashMap<String, WatcherRc>>>>,
}

impl Changes {
    pub fn new() -> Self {
        Changes {
            indexes: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    pub fn keeps_key(&self, kind: Wildcard, key: &str) -> bool {
        self.indexes
            .borrow()
            .get(&kind)
            .is_some_and(|rails| rails.contains_key(key))
    }

    pub fn live(&self) -> Vec<Rc<Interest>> {
        let mut found = Vec::new();
        for index in self.indexes.borrow().values() {
            for watchers in index.values() {
                found.extend(watchers.live());
            }
        }
        found
    }

    pub fn watch(&self, topic: &str, interest: &Rc<Interest>) {
        let parts: Vec<String> = topic.split('.').map(String::from).collect();
        for (kind, key) in lookup_keys(&parts) {
            self.watch_key(kind, &key, interest);
        }
    }

    pub fn notify(&self, pattern: &Pattern) {
        let key = pattern.literal.join(".");
        let found = self
            .indexes
            .borrow()
            .get(&pattern.wildcard)
            .and_then(|watchers| watchers.get(&key).cloned());
        if let Some(watcher) = found {
            watcher.notify();
        }
    }

    fn watch_key(&self, kind: Wildcard, key: &str, interest: &Rc<Interest>) {
        let entry = {
            let mut indexes = self.indexes.borrow_mut();
            let map = indexes.entry(kind).or_default();
            if let Some(source) = map.get(key) {
                Rc::clone(source)
            } else {
                let indexes = Rc::clone(&self.indexes);
                let owned = key.to_string();
                let source = Rc::new_cyclic(|ghost| {
                    let ghost = ghost.clone();
                    let key = owned.clone();
                    Watchers::with_on_empty(Box::new(move || {
                        let mut indexes = indexes.borrow_mut();
                        let Some(map) = indexes.get_mut(&kind) else {
                            return;
                        };
                        if map
                            .get(&key)
                            .is_some_and(|held| Weak::as_ptr(&ghost) == Rc::as_ptr(held))
                        {
                            map.remove(&key);
                        }
                    }))
                });
                map.insert(owned, Rc::clone(&source));
                source
            }
        };
        entry.watch(interest);
    }
}

impl Default for Changes {
    fn default() -> Self {
        Self::new()
    }
}
