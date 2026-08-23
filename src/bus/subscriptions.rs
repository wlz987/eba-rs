use crate::inbox::InboxHandle;
use crate::pattern::{matches, Pattern};
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SubKey {
    inbox_id: u64,
    pat: String,
}

#[derive(Debug)]
struct Entry {
    seq: u64,
    pattern: Pattern,
    inbox: InboxHandle,
}

#[derive(Debug, Default)]
pub struct Subscriptions {
    by_first: HashMap<String, HashMap<SubKey, Entry>>,
    catch_all: HashMap<SubKey, Entry>,
    next_seq: u64,
}

impl Subscriptions {
    pub fn subscribe(&mut self, pattern: &Pattern, inbox: &InboxHandle) {
        let key = self.key(pattern, inbox);
        let exists = match pattern.literal.first() {
            None => self.catch_all.contains_key(&key),
            Some(first) => self
                .by_first
                .get(first)
                .is_some_and(|bucket| bucket.contains_key(&key)),
        };
        if exists {
            return;
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        let bucket = self.writable_bucket(pattern);
        bucket.insert(
            key,
            Entry {
                seq,
                pattern: pattern.clone(),
                inbox: Rc::clone(inbox),
            },
        );
    }

    pub fn unsubscribe(&mut self, pattern: &Pattern, inbox: &InboxHandle) -> bool {
        let key = self.key(pattern, inbox);
        if pattern.literal.is_empty() {
            if self.catch_all.remove(&key).is_none() {
                return false;
            }
            return true;
        }
        let Some(bucket) = self.by_first.get_mut(&pattern.literal[0]) else {
            return false;
        };
        if bucket.remove(&key).is_none() {
            return false;
        }
        if bucket.is_empty() {
            self.by_first.remove(&pattern.literal[0]);
        }
        true
    }

    pub fn snapshot_match(&self, topic_parts: &[&str]) -> Vec<InboxHandle> {
        let mut earliest: HashMap<u64, (InboxHandle, u64)> = HashMap::new();
        let mut collect = |entries: &HashMap<SubKey, Entry>| {
            for entry in entries.values() {
                if !matches(&entry.pattern, topic_parts) {
                    continue;
                }
                let id = entry.inbox.id();
                match earliest.get(&id) {
                    Some((_, seq)) if *seq <= entry.seq => {}
                    _ => {
                        earliest.insert(id, (Rc::clone(&entry.inbox), entry.seq));
                    }
                }
            }
        };
        if !topic_parts.is_empty() {
            if let Some(literal) = self.by_first.get(topic_parts[0]) {
                collect(literal);
            }
        }
        if !self.catch_all.is_empty() {
            collect(&self.catch_all);
        }
        let mut wins: Vec<(InboxHandle, u64)> = earliest.into_values().collect();
        wins.sort_by_key(|(_, seq)| *seq);
        wins.into_iter().map(|(box_, _)| box_).collect()
    }

    fn key(&self, pattern: &Pattern, inbox: &InboxHandle) -> SubKey {
        SubKey {
            inbox_id: inbox.id(),
            pat: pattern_text(pattern),
        }
    }

    fn writable_bucket(&mut self, pattern: &Pattern) -> &mut HashMap<SubKey, Entry> {
        match pattern.literal.first() {
            None => &mut self.catch_all,
            Some(first) => self.by_first.entry(first.clone()).or_default(),
        }
    }
}

fn pattern_text(pattern: &Pattern) -> String {
    let mut parts = pattern.literal.clone();
    match pattern.wildcard {
        crate::pattern::Wildcard::Star => parts.push("*".into()),
        crate::pattern::Wildcard::GlobStar => parts.push("**".into()),
        crate::pattern::Wildcard::None => {}
    }
    parts.join(".")
}
