use std::rc::Rc;

use super::super::Bus;
use crate::collections::IndexedMap;
use crate::mail::inbox::Inbox;
use crate::mail::routes::Routes;
use crate::notify::watch::Interest;
use crate::pattern::parse_pattern;

impl Bus {
    pub fn lists(&self, inbox_id: usize) -> bool {
        self.routes.borrow().by_inbox.contains_key(&inbox_id)
    }

    pub fn bucket(&self, pattern: &str) -> Option<usize> {
        let parsed = parse_pattern(pattern).ok()?;
        self.routes
            .borrow()
            .indexes
            .get(&parsed.wildcard)?
            .get(&Routes::key(&parsed))
            .map(IndexedMap::len)
    }

    pub fn right_count(&self, pattern: &str, inbox: &Rc<Inbox>) -> Option<usize> {
        let parsed = parse_pattern(pattern).ok()?;
        let key = Routes::key(&parsed);
        let routes = self.routes.borrow();
        let entry = routes
            .indexes
            .get(&parsed.wildcard)?
            .get(&key)?
            .get(&inbox.id)?;
        let held = entry.rights.borrow().len();
        Some(held)
    }

    pub fn offer_count(&self) -> usize {
        self.offers.borrow().len()
    }

    pub fn grant_depth(&self) -> usize {
        self.grant_frames.borrow().len()
    }

    pub fn rail(&self) -> Vec<Rc<Interest>> {
        let changes = Rc::clone(&self.routes.borrow().changes);
        changes.live()
    }
}
