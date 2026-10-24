use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;

use eba_rs_infra::collections::IndexedMap;
use eba_rs_infra::envelope::ids::{actor_id, envelope_id};
use eba_rs_infra::envelope::{make_envelope, Envelope, MakeEnvelope};
use eba_rs_infra::errors::{BoxError, EnvelopeBuildError, MailboxFull};
use eba_rs_infra::mail::inbox::Receiver;
use eba_rs_infra::mail::inbox::{pause_work, watch_space};
use eba_rs_infra::mail::{Bus, Inbox};
use eba_rs_infra::notify::signal::Signal;
use eba_rs_infra::notify::watch::Interest;
use eba_rs_infra::Payload;

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

    let full = Inbox::new(1);
    let room = Inbox::new(4);
    let full_side = full.receiver();
    let room_side = room.receiver();
    bus.subscribe("effort.x", &full)?;
    bus.subscribe("effort.*", &room)?;
    bus.publish(&letter("effort.x", "e1"))?;
    let _ = room_side.try_recv();
    let skipped = bus.publish_best_effort(&letter("effort.x", "e2"))?;
    let left = full_side.try_recv().ok().flatten().expect("e1");
    let right = room_side.try_recv().ok().flatten().expect("e2");
    println!(
        "best-effort {} {} {}",
        skipped, left.header.id.0, right.header.id.0
    );

    let latest = Inbox::new(4);
    let latest_side = latest.receiver();
    bus.subscribe("latest.x", &latest)?;
    bus.publish(&letter("latest.x", "l1"))?;
    bus.publish(&letter("latest.x", "l2"))?;
    let stale = bus.publish_latest(&letter("latest.x", "l3"))?;
    let kept = latest_side.try_recv().ok().flatten().expect("latest");
    assert_eq!(stale, 0);
    assert_eq!(kept.header.id.0, "l3");
    assert!(latest_side.try_recv().ok().flatten().is_none());

    let refused = bus.publish_latest(&Rc::new(
        make_envelope(MakeEnvelope {
            topic: String::from("latest.x"),
            payload: Arc::new(1i64) as Payload,
            from: actor_id("x"),
            cause: None,
            id: Some(envelope_id("req-1")),
            kind: String::from("request"),
            reply_to: Some(String::from("sys.reply.i1")),
            correlation_id: None,
        })
        .expect("request"),
    ));
    assert!(refused
        .as_ref()
        .err()
        .is_some_and(|error| error.downcast_ref::<EnvelopeBuildError>().is_some()));

    let busy = Inbox::new(1);
    let busy_side = busy.receiver();
    bus.subscribe("latest.y.*", &busy)?;
    bus.publish(&letter("latest.y.a", "y1"))?;
    assert_eq!(bus.publish_latest(&letter("latest.y.a", "y2"))?, 0);
    let kept = busy_side.try_recv().ok().flatten().expect("replaced");
    assert_eq!(kept.header.id.0, "y2");
    bus.publish(&letter("latest.y.a", "y3"))?;
    assert_eq!(bus.publish_latest(&letter("latest.y.b", "y4"))?, 1);
    let kept = busy_side.try_recv().ok().flatten().expect("blocked");
    assert_eq!(kept.header.id.0, "y3");

    let paused = Inbox::new(2);
    let paused_side = paused.receiver();
    bus.subscribe("latest.p", &paused)?;
    bus.publish(&letter("latest.p", "p1"))?;
    pause_work(&paused, true);
    assert_eq!(bus.publish_latest(&letter("latest.p", "p2"))?, 1);
    pause_work(&paused, false);
    assert_eq!(bus.publish_latest(&letter("latest.p", "p3"))?, 0);
    let kept = paused_side.try_recv().ok().flatten().expect("unpaused");
    assert_eq!(kept.header.id.0, "p3");

    let fired = Rc::new(Cell::new(false));
    let flag = Rc::clone(&fired);
    let watched = Inbox::new(2);
    bus.subscribe("latest.w", &watched)?;
    let interest = Rc::new(Interest::new(
        Rc::new(Signal::new()),
        Rc::new(RefCell::new(IndexedMap::new())),
        String::from("latest.w"),
    ));
    interest.set_on_notify(Rc::new(move || flag.set(true)));
    watch_space(&watched, &interest);
    bus.publish(&letter("latest.w", "w1"))?;
    bus.publish(&letter("latest.w", "w2"))?;
    assert_eq!(bus.publish_latest(&letter("latest.w", "w3"))?, 0);
    assert!(fired.get());
    println!("latest ok");
    Ok(())
}
