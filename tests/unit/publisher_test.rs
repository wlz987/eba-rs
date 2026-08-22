use crate::util::env_n;
use eba::{ActorId, Bus, Inbox, Publisher, Value};

#[test]
fn publisher_has_no_inbox() {
    let pub_ = Publisher::new(ActorId("p".into()));
    let bus = Bus::new();
    let dest = Inbox::new(2);
    bus.subscribe("echo.**", &dest).unwrap();
    pub_.publish(&bus, &env_n("echo.x", 1)).unwrap();
    assert_eq!(dest.try_recv().map(|e| e.payload), Some(Value::Int(1)));
}

#[test]
fn publisher_cannot_subscribe() {
    let pub_ = Publisher::new(ActorId("p".into()));
    assert_eq!(pub_.actor_id, ActorId("p".into()));
}
