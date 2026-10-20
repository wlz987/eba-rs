use alloc::collections::BTreeMap;
use alloc::rc::{Rc, Weak};
use alloc::string::String;
use alloc::vec::Vec;
use core::cell::RefCell;

use super::bus::Subscription;
use super::inbox::Inbox;
use crate::collections::IndexedMap;
use crate::notify::watch::Changes;
use crate::pattern::{lookup_keys, Pattern, Wildcard};

#[derive(Clone)]
pub(crate) struct Entry {
    inbox: Rc<Inbox>,
    pattern: Pattern,
    count: usize,
    pub(crate) rights: Rc<RefCell<Vec<Weak<Subscription>>>>,
}

fn same_right(left: &Weak<Subscription>, right: &Weak<Subscription>) -> bool {
    Weak::as_ptr(left) == Weak::as_ptr(right)
}

pub struct Routes {
    pub(crate) indexes: BTreeMap<Wildcard, BTreeMap<String, IndexedMap<usize, Entry>>>,
    pub(crate) by_inbox: BTreeMap<usize, BTreeMap<(Wildcard, String), Entry>>,
    pub changes: Rc<Changes>,
}

mod grant;

impl Routes {
    pub(crate) fn new() -> Self {
        Routes {
            indexes: BTreeMap::new(),
            by_inbox: BTreeMap::new(),
            changes: Rc::new(Changes::new()),
        }
    }

    pub(crate) fn key(pattern: &Pattern) -> String {
        pattern.literal.join(".")
    }

    fn buckets(&self, parts: &[String]) -> impl Iterator<Item = &IndexedMap<usize, Entry>> {
        lookup_keys(parts)
            .into_iter()
            .filter_map(|(kind, key)| self.indexes.get(&kind).and_then(|routes| routes.get(&key)))
            .filter(|bucket| !bucket.is_empty())
    }

    pub fn snapshot_match(&self, parts: &[String]) -> Vec<Rc<Inbox>> {
        let mut targets: Vec<Rc<Inbox>> = Vec::new();
        for bucket in self.buckets(parts) {
            for entry in bucket.values() {
                let found = &entry.inbox;
                if !targets.iter().any(|box_| Rc::ptr_eq(box_, found)) {
                    targets.push(Rc::clone(found));
                }
            }
        }
        targets
    }
    pub fn hold_count(&self, parts: &[String], inbox: &Rc<Inbox>) -> usize {
        let mut total = 0;
        for bucket in self.buckets(parts) {
            if let Some(entry) = bucket.get(&inbox.id) {
                if Rc::ptr_eq(&entry.inbox, inbox) {
                    total += entry.count;
                }
            }
        }
        total
    }
}
