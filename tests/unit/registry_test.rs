use crate::util::{echo_result, fixed, aaaa, bbbb};
use eba::{
    make_envelope, ok_payload, Registry, State, Value,
};

fn start_params(rid: &eba::EnvelopeId) -> eba::StartParams {
    eba::StartParams {
        request_prefix: "acl".into(),
        result_prefix: "acl.result".into(),
        payload: Value::Null,
        from: "h".into(),
        cause: rid.clone(),
        request_id: None,
    }
}

fn started() -> (eba::BusHandle, eba::InboxHandle, Registry, eba::EnvelopeId) {
    let bus = eba::Bus::new();
    let box_ = eba::Inbox::new(4);
    let mut reg = Registry::new();
    let rid = aaaa();
    reg.start_request(&bus, &box_, &fixed(&rid.0), start_params(&rid))
        .unwrap();
    (bus, box_, reg, rid)
}

#[test]
fn quad_miss_keeps_pending() {
    let (_bus, _box, mut reg, rid) = started();
    let echo = echo_result(&rid, &bbbb(), 1);
    let out = reg.resolve_only(&echo).unwrap();
    assert!(!out.fresh);
    assert_eq!(reg.state(&rid), Some(State::Pending));
}

#[test]
fn quad_hit_fresh() {
    let (_bus, _box, mut reg, rid) = started();
    let reply = echo_result(&rid, &rid, 7);
    let out = reg.resolve_only(&reply).unwrap();
    assert!(out.fresh);
    assert_eq!(out.state, Some(State::Resolved));
}

#[test]
fn finish_safe_unsubscribes() {
    let (bus, box_, mut reg, rid) = started();
    reg.finish_safe(&bus, &box_, &rid);
    bus.publish(&echo_result(&rid, &rid, 1)).unwrap();
    assert!(box_.try_recv().is_none());
}

#[test]
fn late_echo_not_fresh_then_finish() {
    let (bus, box_, mut reg, rid) = started();
    let reply = echo_result(&rid, &rid, 7);
    assert!(reg.resolve_only(&reply).unwrap().fresh);
    let again = reg.resolve_only(&reply).unwrap();
    assert!(!again.fresh);
    assert_eq!(again.state, Some(State::Resolved));
    reg.finish_safe(&bus, &box_, &rid);
    assert_eq!(reg.state(&rid), None);
}

#[test]
fn resolve_without_bind_raises() {
    let env = make_envelope(
        "x",
        Value::Int(1),
        "a".into(),
        eba::MakeOptions {
            id: Some(aaaa()),
            ..Default::default()
        },
    )
    .unwrap();
    let mut reg = Registry::new();
    let err = reg.resolve_only(&env).unwrap_err();
    assert!(matches!(err, eba::Error::State(_)));
    assert!(err.to_string().contains("IdGen"));
}

#[test]
fn start_request_publish_full_cleans_up() {
    let bus = eba::Bus::new();
    let dest = eba::Inbox::new(1);
    let pad = make_envelope(
        "pad",
        Value::Int(0),
        "a".into(),
        eba::MakeOptions {
            id: Some(crate::util::cccc()),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(dest.try_enqueue(pad));
    bus.subscribe("acl.**", &dest).unwrap();
    let box_ = eba::Inbox::new(4);
    let mut reg = Registry::new();
    let rid = aaaa();
    let err = reg
        .start_request(&bus, &box_, &fixed(&rid.0), start_params(&rid))
        .unwrap_err();
    assert!(matches!(err, eba::Error::MailboxFull));
    assert_eq!(reg.state(&rid), None);
}

#[test]
fn topic_suffix_miss() {
    let (_bus, _box, mut reg, rid) = started();
    let echo = make_envelope(
        "acl.result.other",
        crate::util::object(vec![
            ("request_id", Value::Str(rid.0.clone())),
            ("result", ok_payload(Value::Int(1))),
        ]),
        "l".into(),
        eba::MakeOptions {
            cause: Some(rid.clone()),
            id_gen: Some(crate::util::seq_gen(1)),
            ..Default::default()
        },
    )
    .unwrap();
    let out = reg.resolve_only(&echo).unwrap();
    assert!(!out.fresh);
    assert_eq!(reg.state(&rid), Some(State::Pending));
}
