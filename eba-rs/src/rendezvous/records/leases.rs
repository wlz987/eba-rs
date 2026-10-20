use std::collections::HashMap;
use std::rc::Rc;

use super::{active_of, right, same_lease, same_target, LeaseRef, Records, Route};
use crate::errors::BoxError;
use crate::mail::bus::{Bus, BusCount, GrantFrame, Lease};
use crate::mail::inbox::Inbox;

impl<C> Records<C> {
    pub(super) fn grant(
        &mut self,
        bus: &Bus,
        inbox: &Rc<Inbox>,
        topic: &str,
        counter: BusCount,
    ) -> Result<Route, BoxError> {
        let routes = self.ensure_routes(inbox.id);
        routes.remove(topic);
        let depth = bus.open_grants();
        let outcome = (|| -> Result<Route, BoxError> {
            let target = bus.subscribe_lease(topic, inbox)?;
            let owner = right(inbox.id, &target, &bus.grant_frame(depth))?;
            Ok(Route {
                lease: LeaseRef { target, owner },
                count: 0,
                bus: counter,
            })
        })();
        match outcome {
            Ok(route) => {
                bus.close_grants();
                Ok(route)
            }
            Err(error) => {
                let frame = bus.grant_frame(depth);
                self.abandon(&frame, inbox.id);
                bus.close_grants();
                if self
                    .routes
                    .get(&inbox.id)
                    .is_none_or(|found| found.is_empty())
                {
                    self.routes.remove(&inbox.id);
                }
                Err(error)
            }
        }
    }

    pub(super) fn abandon(&mut self, frame: &GrantFrame, inbox_id: usize) {
        let mut targets: Vec<Rc<dyn Lease>> = Vec::new();
        for (_, granted) in frame.iter().filter(|(id, _)| *id == inbox_id) {
            if !targets.iter().any(|held| Rc::ptr_eq(held, granted)) {
                targets.push(Rc::clone(granted));
            }
        }
        for target in targets {
            if target.close().is_err() {
                let Ok(owner) = right(inbox_id, &target, frame) else {
                    continue;
                };
                if !active_of(&owner) {
                    continue;
                }
                self.remember(LeaseRef { target, owner });
            }
        }
    }

    pub(super) fn remember(&mut self, lease: LeaseRef) {
        if !self.stuck.iter().any(|item| same_target(item, &lease)) {
            self.stuck.push(lease);
        }
    }

    pub(super) fn forget(&mut self, lease: &LeaseRef) {
        self.stuck.retain(|item| !same_target(item, lease));
    }

    pub(super) fn ensure_routes(&mut self, inbox_id: usize) -> &mut HashMap<String, Route> {
        self.routes.entry(inbox_id).or_default()
    }

    pub(super) fn find_route(&self, target: &Rc<dyn Lease>) -> Option<(usize, String)> {
        for (inbox_id, routes) in &self.routes {
            for (topic, route) in routes {
                if same_lease(&route.lease.target, target) {
                    return Some((*inbox_id, topic.clone()));
                }
            }
        }
        None
    }

    pub(super) fn drop_route(&mut self, inbox_id: usize, topic: &str) {
        let mut empty = false;
        if let Some(routes) = self.routes.get_mut(&inbox_id) {
            routes.remove(topic);
            empty = routes.is_empty();
        }
        if empty {
            self.routes.remove(&inbox_id);
        }
    }

    pub(super) fn retry(&mut self) {
        let pending = std::mem::take(&mut self.stuck);
        let mut kept: Vec<LeaseRef> = Vec::new();
        for lease in pending {
            let found = self.find_route(&lease.target);
            if !active_of(&lease.owner) {
                if let Some((inbox_id, topic)) = found {
                    let spent = self
                        .routes
                        .get(&inbox_id)
                        .and_then(|routes| routes.get(&topic))
                        .is_some_and(|route| route.count == 0);
                    if spent {
                        self.drop_route(inbox_id, &topic);
                    }
                }
                continue;
            }
            if let Some((inbox_id, topic)) = &found {
                let busy = self
                    .routes
                    .get(inbox_id)
                    .and_then(|routes| routes.get(topic))
                    .is_some_and(|route| route.count > 0);
                if busy {
                    kept.push(lease);
                    continue;
                }
            }
            if lease.target.close().is_err() {
                kept.push(lease);
                continue;
            }
            if active_of(&lease.owner) {
                kept.push(lease);
                continue;
            }
            if let Some((inbox_id, topic)) = found {
                self.drop_route(inbox_id, &topic);
            }
        }
        self.stuck = kept;
    }
}
