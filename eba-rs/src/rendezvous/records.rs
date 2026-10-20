use std::collections::HashMap;
use std::rc::Rc;

use crate::envelope::result::{result_body, result_request_id, Completion};
use crate::envelope::Address;
use crate::envelope::{Envelope, EnvelopeId};
use crate::errors::{combine_failures, BoxError, BusError, EnvelopeBuildError};
use crate::mail::bus::{Bus, BusCount, GrantFrame, Lease};
use crate::mail::inbox::Inbox;
use crate::time::deadlines::ELAPSED;
use crate::time::numbers::nonnegative_int;
use crate::time::Deadlines;

mod leases;

fn lease_ptr(lease: &Rc<dyn Lease>) -> *const () {
    Rc::as_ptr(lease).cast::<()>()
}

fn same_lease(left: &Rc<dyn Lease>, right: &Rc<dyn Lease>) -> bool {
    lease_ptr(left) == lease_ptr(right)
}

#[derive(Clone)]
pub struct LeaseRef {
    pub target: Rc<dyn Lease>,
    pub owner: Rc<dyn Lease>,
}

fn active_of(lease: &Rc<dyn Lease>) -> bool {
    lease.active().unwrap_or(false)
}

fn same_target(left: &LeaseRef, right: &LeaseRef) -> bool {
    lease_ptr(&left.target) == lease_ptr(&right.target)
}

fn noted(inbox_id: usize, frame: &GrantFrame) -> Vec<Rc<dyn Lease>> {
    frame
        .iter()
        .filter(|(id, _)| *id == inbox_id)
        .map(|(_, granted)| Rc::clone(granted))
        .collect()
}

pub fn matched(granted: &Rc<dyn Lease>, owned: &[Rc<dyn Lease>]) -> Option<Rc<dyn Lease>> {
    let address: Option<*const ()> = granted.subscription_ptr().map(|found| found.cast());
    if let Some(address) = address {
        for held in owned {
            if held.subscription_ptr().map(|found| found.cast()) == Some(address) {
                return Some(Rc::clone(held));
            }
        }
    }
    if owned.len() == 1 {
        return Some(Rc::clone(&owned[0]));
    }
    None
}

fn right(
    inbox_id: usize,
    lease: &Rc<dyn Lease>,
    frame: &GrantFrame,
) -> Result<Rc<dyn Lease>, BoxError> {
    let owned = noted(inbox_id, frame);
    matched(lease, &owned)
        .ok_or_else(|| Box::from(BusError("lease does not carry its subscription".into())))
}

#[derive(Clone)]
pub struct Route {
    pub lease: LeaseRef,
    pub count: usize,
    pub bus: BusCount,
}

impl Route {
    fn owner(&self) -> Rc<dyn Lease> {
        Rc::clone(&self.lease.owner)
    }
}

pub struct Record<C> {
    pub topic: String,
    pub cause: EnvelopeId,
    pub continuation: C,
    pub inbox: Rc<Inbox>,
}

pub struct Claim<C> {
    pub continuation: C,
    pub completion: Completion,
}

pub struct EntryLedger {
    pub request_id: EnvelopeId,
    pub topic: String,
    pub inbox_id: usize,
    pub deadline_ms: Option<u64>,
}

pub struct RouteLedger {
    pub inbox_id: usize,
    pub topic: String,
    pub count: usize,
}

pub struct RecordsLedger {
    pub entries: Vec<EntryLedger>,
    pub routes: Vec<RouteLedger>,
    pub timers: Vec<(EnvelopeId, u64)>,
}

#[derive(Debug)]
pub struct UnreadableResult;
impl std::fmt::Display for UnreadableResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unreadable result")
    }
}
impl std::error::Error for UnreadableResult {}

pub struct Records<C> {
    entries: HashMap<EnvelopeId, Record<C>>,
    deadlines: Deadlines<EnvelopeId>,
    routes: HashMap<usize, HashMap<String, Route>>,
    stuck: Vec<LeaseRef>,
}

impl<C> Default for Records<C> {
    fn default() -> Self {
        Records::new()
    }
}

impl<C> Records<C> {
    pub fn new() -> Self {
        Records {
            entries: HashMap::new(),
            deadlines: Deadlines::new(),
            routes: HashMap::new(),
            stuck: Vec::new(),
        }
    }

    pub fn size(&self) -> usize {
        self.entries.len()
    }

    pub fn next_deadline_ms(&self) -> Option<u64> {
        self.deadlines.next_ms()
    }

    pub fn elapse_open(&mut self) {
        for request_id in self.entries.keys().cloned().collect::<Vec<_>>() {
            if self.deadlines.at(&request_id).is_none() {
                self.deadlines.add(request_id, ELAPSED);
            }
        }
    }

    pub fn unreleased(&self) -> usize {
        self.stuck.len()
    }

    pub fn continuation(&self, request_id: &EnvelopeId) -> Option<&C> {
        self.entries
            .get(request_id)
            .map(|record| &record.continuation)
    }

    pub fn inspect(&self) -> RecordsLedger {
        let entries = self
            .entries
            .iter()
            .map(|(request_id, record)| EntryLedger {
                request_id: request_id.clone(),
                topic: record.topic.clone(),
                inbox_id: record.inbox.id,
                deadline_ms: self.deadlines.at(request_id),
            })
            .collect();
        let mut routes = Vec::new();
        for (inbox_id, table) in &self.routes {
            for (topic, route) in table {
                routes.push(RouteLedger {
                    inbox_id: *inbox_id,
                    topic: topic.clone(),
                    count: route.count,
                });
            }
        }
        RecordsLedger {
            entries,
            routes,
            timers: self.deadlines.timed(),
        }
    }

    pub fn submit(
        &mut self,
        bus: &Bus,
        inbox: &Rc<Inbox>,
        request: Rc<Envelope>,
        continuation: C,
        deadline_ms: Option<u64>,
    ) -> Result<(), BoxError> {
        let header = &request.header;
        let reply_to = match &header.address {
            Address::Request { reply_to } => reply_to.clone(),
            _ => {
                return Err(Box::from(EnvelopeBuildError(
                    "expected a request with reply_to".into(),
                )))
            }
        };
        if self.entries.contains_key(&header.id) {
            return Err(Box::from(EnvelopeBuildError(format!(
                "duplicate request id: {}",
                header.id
            ))));
        }
        if let Some(deadline_ms) = deadline_ms {
            nonnegative_int(deadline_ms, "deadline_ms");
        }
        if inbox.is_closed() {
            return Err(Box::from(BusError("inbox closed".into())));
        }
        let counter = bus.counter();
        let existing = self.reuse_route(inbox.id, &reply_to);
        let mut route = match existing {
            Some(route) => route,
            None => self.grant(bus, inbox, &reply_to, counter)?,
        };
        route.count += 1;
        self.ensure_routes(inbox.id)
            .insert(reply_to.clone(), route.clone());
        self.entries.insert(
            header.id.clone(),
            Record {
                topic: reply_to,
                cause: header.cause.clone(),
                continuation,
                inbox: Rc::clone(inbox),
            },
        );
        bus.hold_wait(&header.id);
        let delivered = match bus.publish(&request) {
            Ok(delivered) => delivered,
            Err(error) => {
                return match self.finish(&header.id, false) {
                    Ok(_) => Err(error),
                    Err(secondary) => Err(combine_failures(error, secondary)),
                };
            }
        };
        let mut due = deadline_ms;
        if !delivered && due.is_none() {
            due = Some(ELAPSED);
        }
        if let Some(due) = due {
            self.deadlines.add(header.id.clone(), due);
        }
        Ok(())
    }

    fn reuse_route(&mut self, inbox_id: usize, topic: &str) -> Option<Route> {
        let dead = {
            let routes = self.routes.get(&inbox_id)?;
            let route = routes.get(topic)?;
            route.count == 0 && !active_of(&route.owner())
        };
        if !dead {
            let route = self.routes.get(&inbox_id)?.get(topic)?.clone();
            return Some(route);
        }
        let dead_lease = {
            let route = self.routes.get(&inbox_id)?.get(topic)?;
            route.lease.clone()
        };
        self.forget(&dead_lease);
        self.drop_route(inbox_id, topic);
        None
    }

    pub fn take_result(&mut self, envelope: &Rc<Envelope>) -> Result<Option<Claim<C>>, BoxError> {
        let Some(request_id) = result_request_id(envelope) else {
            return Ok(None);
        };
        {
            let Some(record) = self.entries.get(&request_id) else {
                return Ok(None);
            };
            if record.topic != envelope.header.topic || record.cause != envelope.header.cause {
                return Ok(None);
            }
        }
        let Some(body) = result_body(envelope) else {
            return Err(Box::from(UnreadableResult));
        };
        let body = body.clone();
        let Some(record) = self.finish(&request_id, true)? else {
            return Ok(None);
        };
        Ok(Some(Claim {
            continuation: record.continuation,
            completion: Completion {
                request_id,
                body: Some(body),
            },
        }))
    }

    pub fn take_due(&mut self, now_ms: u64) -> Option<Claim<C>> {
        nonnegative_int(now_ms, "clock_ms");
        let request_id = self.deadlines.peek_due(now_ms).cloned()?;
        let record = self.finish(&request_id, true).ok()??;
        Some(Claim {
            continuation: record.continuation,
            completion: Completion {
                request_id,
                body: None,
            },
        })
    }

    pub fn recover(&mut self) {
        self.retry();
    }

    pub fn finish(
        &mut self,
        request_id: &EnvelopeId,
        claim: bool,
    ) -> Result<Option<Record<C>>, BoxError> {
        self.retry();
        let Some(record) = self.entries.get(request_id) else {
            return Ok(None);
        };
        let inbox_id = record.inbox.id;
        let topic = record.topic.clone();
        let inbox = Rc::clone(&record.inbox);
        let mut closed = true;
        let mut stuck_lease: Option<LeaseRef> = None;
        if let Some(route) = self
            .routes
            .get_mut(&inbox_id)
            .and_then(|routes| routes.get_mut(&topic))
        {
            if route.count == 1 {
                let before = route.bus.subscription_count(&topic, &inbox);
                let lease = route.lease.clone();
                if let Err(error) = lease.target.close() {
                    if !claim {
                        return Err(error);
                    }
                    let after = route.bus.subscription_count(&topic, &inbox);
                    closed = !active_of(&lease.owner) || after < before;
                    if !closed {
                        stuck_lease = Some(lease);
                    }
                }
            }
        }
        if let Some(lease) = stuck_lease {
            self.remember(lease);
        }
        let Some(record) = self.entries.remove(request_id) else {
            return Ok(None);
        };
        self.deadlines.remove(request_id);
        let route = self
            .routes
            .get(&inbox_id)
            .and_then(|routes| routes.get(&topic))
            .cloned();
        if let Some(route) = route {
            route.bus.release_wait(request_id);
            let remaining = route.count.saturating_sub(1);
            if remaining == 0 && closed {
                self.drop_route(inbox_id, &topic);
                self.forget(&route.lease);
            } else {
                let mut kept = route.clone();
                kept.count = remaining;
                if let Some(routes) = self.routes.get_mut(&inbox_id) {
                    routes.insert(topic.clone(), kept);
                }
            }
        }
        if claim {
            self.retry();
        }
        Ok(Some(record))
    }
}
