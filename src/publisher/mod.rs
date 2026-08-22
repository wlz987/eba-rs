
use crate::bus::Bus;
use crate::envelope::{ActorId, Envelope};

#[derive(Debug)]
pub struct Publisher {
    pub actor_id: ActorId,
}

impl Publisher {
    pub fn new(actor_id: ActorId) -> Publisher {
        Publisher { actor_id }
    }

    pub fn publish(&self, bus: &Bus, envelope: &Envelope) -> crate::Result<()> {
        bus.publish(envelope)
    }
}
