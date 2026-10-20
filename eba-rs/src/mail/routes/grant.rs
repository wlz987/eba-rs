use std::cell::RefCell;
use std::rc::{Rc, Weak};

use super::{same_right, Entry, Routes};
use crate::mail::bus::Subscription;
use crate::mail::inbox::{forget_route, remember_route, Inbox, RouteRevoke};
use crate::pattern::Pattern;

impl Routes {
    pub fn subscribe(
        &mut self,
        pattern: Pattern,
        inbox: &Rc<Inbox>,
        revoke: &Rc<RouteRevoke>,
    ) -> Option<Pattern> {
        let key = Self::key(&pattern);
        let wildcard = pattern.wildcard;
        let added;
        let held;
        {
            let bucket = self
                .indexes
                .entry(wildcard)
                .or_default()
                .entry(key.clone())
                .or_default();
            if !bucket.contains_key(&inbox.id) {
                bucket.insert(
                    inbox.id,
                    Entry {
                        inbox: Rc::clone(inbox),
                        pattern: pattern.clone(),
                        count: 0,
                        rights: Rc::new(RefCell::new(Vec::new())),
                    },
                );
            }
            let entry = bucket.get_mut(&inbox.id).expect("bucket holds its own key");
            added = entry.count == 0;
            entry.count += 1;
            held = entry.clone();
        }
        if !added {
            return None;
        }
        self.by_inbox
            .entry(inbox.id)
            .or_default()
            .insert((wildcard, key), held);
        remember_route(inbox, revoke);
        Some(pattern)
    }

    pub fn bind(&mut self, pattern: &Pattern, inbox: &Rc<Inbox>, right: &Weak<Subscription>) {
        if let Some(entry) = self.route_entry(pattern, inbox) {
            entry.rights.borrow_mut().push(right.clone());
        }
    }

    fn route_entry(&mut self, pattern: &Pattern, inbox: &Rc<Inbox>) -> Option<&mut Entry> {
        self.indexes
            .get_mut(&pattern.wildcard)
            .and_then(|index| index.get_mut(&Self::key(pattern)))
            .and_then(|bucket| bucket.get_mut(&inbox.id))
    }

    fn revoke_rights(entry: &Entry) {
        for right in entry.rights.borrow().iter().filter_map(Weak::upgrade) {
            right.disarm();
        }
        entry.rights.borrow_mut().clear();
    }

    pub fn detach(
        &mut self,
        pattern: &Pattern,
        inbox: &Rc<Inbox>,
        revoke: &Rc<RouteRevoke>,
        right: Option<&Weak<Subscription>>,
    ) -> (bool, Option<Pattern>) {
        let key = Self::key(pattern);
        let wildcard = pattern.wildcard;
        let disarm = |right: Option<&Weak<Subscription>>| {
            if let Some(right) = right.and_then(Weak::upgrade) {
                right.disarm();
            }
        };
        let entry = match self
            .indexes
            .get_mut(&wildcard)
            .and_then(|index| index.get_mut(&key))
            .and_then(|bucket| bucket.get_mut(&inbox.id))
        {
            Some(entry) => {
                if let Some(right) = right {
                    entry
                        .rights
                        .borrow_mut()
                        .retain(|held| !same_right(held, right));
                }
                entry.clone()
            }
            None => {
                disarm(right);
                return (false, None);
            }
        };
        if entry.count > 1 {
            if let Some(entry) = self
                .indexes
                .get_mut(&wildcard)
                .and_then(|index| index.get_mut(&key))
                .and_then(|bucket| bucket.get_mut(&inbox.id))
            {
                entry.count -= 1;
            }
            disarm(right);
            return (true, None);
        }
        if let Some(bucket) = self
            .indexes
            .get_mut(&wildcard)
            .and_then(|index| index.get_mut(&key))
        {
            bucket.remove(&inbox.id);
            if bucket.is_empty() {
                self.indexes.get_mut(&wildcard).unwrap().remove(&key);
            }
        }
        if let Some(group) = self.by_inbox.get_mut(&inbox.id) {
            group.remove(&(wildcard, key.clone()));
            if group.is_empty() {
                self.by_inbox.remove(&inbox.id);
                forget_route(inbox, revoke);
            }
        }
        disarm(right);
        (true, Some(entry.pattern))
    }

    pub fn unsubscribe_all(&mut self, inbox: &Rc<Inbox>) -> Vec<Pattern> {
        let Some(group) = self.by_inbox.remove(&inbox.id) else {
            return Vec::new();
        };
        let mut patterns = Vec::new();
        for ((wildcard, key), entry) in group {
            if let Some(bucket) = self
                .indexes
                .get_mut(&wildcard)
                .and_then(|m| m.get_mut(&key))
            {
                bucket.remove(&inbox.id);
                if bucket.is_empty() {
                    self.indexes.get_mut(&wildcard).unwrap().remove(&key);
                }
            }
            Self::revoke_rights(&entry);
            patterns.push(entry.pattern);
        }
        patterns
    }
}
