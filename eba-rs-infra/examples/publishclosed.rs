use std::rc::Rc;
use std::sync::Arc;

use eba_rs_infra::envelope::ids::{actor_id, envelope_id};
use eba_rs_infra::envelope::{make_envelope, MakeEnvelope};
use eba_rs_infra::errors::BoxError;
use eba_rs_infra::mail::{Bus, Inbox};
use eba_rs_infra::Payload;

fn main() -> Result<(), BoxError> {
    let bus = Bus::new();
    let inbox = Inbox::new(8);
    let receiver = inbox.receiver();
    bus.subscribe("a.b", &inbox)?;
    receiver.close();
    let mut options = MakeEnvelope::event("a.b", Arc::new(1u8) as Payload, actor_id("c"));
    options.id = Some(envelope_id("e1"));
    let envelope = Rc::new(make_envelope(options)?);
    let delivered = bus.publish(&envelope)?;
    if delivered {
        return Err("publishclosed leaked".into());
    }
    println!("publishclosed: {delivered}");
    Ok(())
}
