use std::rc::Rc;
use std::sync::Arc;

use eba_rs::envelope::ids::{actor_id, envelope_id};
use eba_rs::envelope::{make_envelope, Envelope, MakeEnvelope};
use eba_rs::errors::{BoxError, MailboxFull};
use eba_rs::mail::inbox::Receiver;
use eba_rs::mail::{Bus, Inbox};
use eba_rs::Payload;

fn letter(topic: &str, id: &str) -> Rc<Envelope> {
    let mut options = MakeEnvelope::event(
        topic,
        Arc::new(String::from("sold-out")) as Payload,
        actor_id("order"),
    );
    options.id = Some(envelope_id(id));
    Rc::new(make_envelope(options).expect("一封信"))
}

fn take(boxes: &[(&'static str, Receiver)]) -> Vec<(&'static str, String)> {
    let mut found = Vec::new();
    for (name, receiver) in boxes {
        if let Some(item) = receiver.try_recv().ok().flatten() {
            found.push((*name, item.header.id.0.clone()));
        }
    }
    found
}

fn main() -> Result<(), BoxError> {
    let bus = Bus::new();
    let inventory = Inbox::new(1);
    let audit = Inbox::new(4);
    let overview = Inbox::new(4);
    let boxes = [
        ("库存", inventory.receiver()),
        ("旁听", audit.receiver()),
        ("总览", overview.receiver()),
    ];
    bus.subscribe("inventory.*", &inventory)?;
    bus.subscribe("inventory.reserve", &audit)?;
    bus.subscribe("inventory.**", &overview)?;

    bus.publish(&letter("inventory.reserve", "child-42"))?;
    println!("inventory.reserve {:?}", take(&boxes));
    bus.publish(&letter("inventory.reserve.extra", "child-43"))?;
    println!("inventory.reserve.extra {:?}", take(&boxes));
    bus.publish(&letter("inventory.reserve", "child-44"))?;
    let outcome = bus.publish(&letter("inventory.reserve", "child-45"));
    match outcome {
        Err(refused) if refused.downcast_ref::<MailboxFull>().is_some() => {
            println!("库存已满 {:?}", take(&boxes));
        }
        Err(refused) => return Err(refused),
        Ok(_) => {}
    }
    Ok(())
}
