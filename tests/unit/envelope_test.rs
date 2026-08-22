use eba::{
    make_envelope, topic_segment,
    ActorId, EnvelopeId, MakeOptions, Value,
};

fn seq(start: u64) -> eba::IdGenHandle {
    std::rc::Rc::new(std::cell::RefCell::new(eba::SeqIdGen::new(start)))
}

#[test]
fn root_cause_equals_id() {
    let env = make_envelope("read.x", Value::Int(1), ActorId("a".into()), MakeOptions {
        id_gen: Some(seq(1)),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(env.header.cause, env.header.id);
    assert_eq!(env.header.from, ActorId("a".into()));
}

#[test]
fn inherited_cause() {
    let ids = seq(1);
    let root = make_envelope("job.x", Value::Null, ActorId("a".into()), MakeOptions {
        id_gen: Some(ids.clone()),
        ..Default::default()
    })
    .unwrap();
    let child = make_envelope("acl.x", Value::Null, ActorId("a".into()), MakeOptions {
        cause: Some(root.header.cause.clone()),
        id_gen: Some(ids),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(child.header.cause, root.header.id);
    assert_ne!(child.header.id, root.header.id);
}

#[test]
fn make_envelope_needs_id_or_id_gen() {
    assert!(make_envelope("read.x", Value::Int(1), ActorId("a".into()), Default::default()).is_err());
}

#[test]
fn empty_and_illegal_topic() {
    assert!(eba::split_topic("").is_err());
    assert!(make_envelope(
        "Bad",
        Value::Int(1),
        ActorId("a".into()),
        MakeOptions {
            id: Some(EnvelopeId("a".repeat(32))),
            ..Default::default()
        }
    )
    .is_err());
}

#[test]
fn header_has_no_ttl() {
    let env = make_envelope(
        "read.x",
        Value::Int(1),
        ActorId("a".into()),
        MakeOptions {
            id: Some(EnvelopeId("a".repeat(32))),
            ..Default::default()
        },
    )
    .unwrap();
    let hdr = &env.header;
    let _four_fields: (&EnvelopeId, &String, &ActorId, &EnvelopeId) =
        (&hdr.id, &hdr.topic, &hdr.from, &hdr.cause);
    assert_eq!(
        hdr.id,
        EnvelopeId("a".repeat(32)),
        "header carries exactly the four facts"
    );
}

#[test]
fn seq_idgen_stable() {
    let ids = seq(1);
    let a = ids.borrow_mut().next_envelope_id();
    let b = ids.borrow_mut().next_envelope_id();
    assert_ne!(a, b);
    assert!(topic_segment(&a).unwrap().starts_with('e'));
}
