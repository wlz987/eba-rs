
use crate::bus::Bus;
use crate::envelope::{ActorId, Envelope};
use crate::inbox::InboxHandle;
use std::rc::Rc;
use std::string::String;
use std::vec::Vec;

#[derive(Debug)]
pub struct Subscriber {
    pub actor_id: ActorId,
    pub inbox: InboxHandle,
    pub patterns: Vec<String>,
}

impl Subscriber {
    pub fn new(actor_id: ActorId, inbox: &InboxHandle, patterns: &[&str]) -> Subscriber {
        inbox.note_reader(actor_id.clone());
        Subscriber {
            actor_id,
            inbox: Rc::clone(inbox),
            patterns: patterns.iter().map(|p| (*p).into()).collect(),
        }
    }

    pub fn start(&self, bus: &Bus) -> crate::Result<()> {
        for p in &self.patterns {
            bus.subscribe(p, &self.inbox)?;
        }
        Ok(())
    }

    pub fn try_recv(&self) -> Option<Envelope> {
        self.inbox.try_recv()
    }
}
