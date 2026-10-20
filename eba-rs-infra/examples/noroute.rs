use std::rc::Rc;
use std::sync::Arc;

use eba_rs_infra::envelope::ids::{actor_id, envelope_id};
use eba_rs_infra::envelope::{make_envelope, MakeEnvelope};
use eba_rs_infra::errors::BoxError;
use eba_rs_infra::mail::Bus;
use eba_rs_infra::Payload;

fn main() -> Result<(), BoxError> {
    let mut options = MakeEnvelope::event("x.y", Arc::new(1u8) as Payload, actor_id("c"));
    options.id = Some(envelope_id("e1"));
    let envelope = Rc::new(make_envelope(options)?);
    let delivered = Bus::new().publish(&envelope)?;
    if delivered {
        return Err("noroute leaked".into());
    }
    println!("noroute: {delivered}");
    Ok(())
}
