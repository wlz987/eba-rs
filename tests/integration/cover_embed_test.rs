
use crate::util::{actor_a, seq};
use eba::{make_envelope, Bus, Inbox, MakeOptions, Publisher, Subscriber, Value};

#[test]
fn subscriber_start_and_recv() {
    let bus = Bus::new();
    let box_ = Inbox::new(2);
    let sub = Subscriber::new(actor_a(), &box_, &["echo.**"]);
    sub.start(&bus).unwrap();
    bus.publish(&make_envelope(
        "echo.x",
        Value::Int(1),
        actor_a(),
        MakeOptions {
            id: Some(eba::EnvelopeId("a".repeat(32))),
            ..Default::default()
        },
    )
    .unwrap())
    .unwrap();
    let got = sub.try_recv().expect("subscriber must receive the letter");
    assert_eq!(got.payload, Value::Int(1));
}

#[test]
fn publisher_publish_only() {
    let bus = Bus::new();
    let dest = Inbox::new(2);
    bus.subscribe("echo.**", &dest).unwrap();
    let pub_ = Publisher::new(actor_a());
    pub_
        .publish(
            &bus,
            &make_envelope(
                "echo.x",
                Value::Str("hi".into()),
                actor_a(),
                MakeOptions {
                    id_gen: Some(seq(1)),
                    ..Default::default()
                },
            )
            .unwrap(),
        )
        .unwrap();
    let got = dest.try_recv().expect("publisher letter delivered");
    assert_eq!(got.payload, Value::Str("hi".into()));
}
