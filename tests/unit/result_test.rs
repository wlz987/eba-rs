use crate::util::{fixed, object, remap, seq_gen};
use eba::{
    err_payload, is_result_err, is_result_ok, looks_like_result_envelope, make_envelope,
    ok_payload, result_body, result_error, result_request_id, result_value, timeout_body, ActorId,
    MakeOptions, Registry, Value,
};

#[test]
fn ok_and_err_shape() {
    let ok = ok_payload(Value::Int(7));
    let err = err_payload("nope", &[("extra", Value::Int(1))]).unwrap();
    assert!(is_result_ok(&ok));
    assert_eq!(result_value(&ok), Some(&Value::Int(7)));
    assert!(is_result_err(&err));
    assert_eq!(result_error(&err), Some("nope"));
    assert!(result_value(&err).is_none());
    assert!(result_error(&ok).is_none());
    assert!(err_payload("x", &[("ok", Value::Bool(false))]).is_err());
}

#[test]
fn timeout_body_is_err() {
    assert!(is_result_err(timeout_body()));
    assert_eq!(result_error(timeout_body()), Some("request_timeout"));
}

#[test]
fn looks_like_needs_id_segment() {
    let rid = crate::util::aaaa();
    let gen = fixed(&rid.0);
    let good = make_envelope(
        &eba::result_topic_of(&rid, "acl.result", &gen).unwrap(),
        object(vec![
            ("request_id", Value::Str(rid.0.clone())),
            ("result", ok_payload(Value::Int(1))),
        ]),
        ActorId("l".into()),
        MakeOptions {
            cause: Some(rid.clone()),
            id_gen: Some(seq_gen(1)),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(looks_like_result_envelope(&good, &gen));
    assert_eq!(result_request_id(&good), Some(rid.clone()));
    assert!(result_body(&good).is_some());
    let bad = make_envelope(
        "acl.result.gone",
        object(vec![
            ("request_id", Value::Str(rid.0.clone())),
            ("result", ok_payload(Value::Int(1))),
        ]),
        ActorId("l".into()),
        MakeOptions {
            cause: Some(rid.clone()),
            id_gen: Some(seq_gen(1)),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(!looks_like_result_envelope(&bad, &gen));
    let plain = make_envelope(
        "x",
        Value::Int(1),
        ActorId("a".into()),
        MakeOptions {
            id_gen: Some(seq_gen(1)),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(result_body(&plain).is_none());
    let mapped = make_envelope(
        "acl.result.za",
        object(vec![
            ("request_id", Value::Str(rid.0.clone())),
            ("result", ok_payload(Value::Int(1))),
        ]),
        ActorId("l".into()),
        MakeOptions {
            cause: Some(rid.clone()),
            id_gen: Some(seq_gen(1)),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(looks_like_result_envelope(&mapped, &remap()));
    assert!(!looks_like_result_envelope(&good, &remap()));
}

#[test]
fn registry_lifecycle_basics() {
    let bus = eba::Bus::new();
    let box_ = eba::Inbox::new(4);
    let mut reg = Registry::new();
    let rid = crate::util::aaaa();
    reg.start_request(
        &bus,
        &box_,
        &fixed(&rid.0),
        eba::StartParams {
            request_prefix: "acl".into(),
            result_prefix: "acl.result".into(),
            payload: Value::Null,
            from: ActorId("h".into()),
            cause: rid.clone(),
            request_id: None,
        },
    )
    .unwrap();
    assert_eq!(reg.state(&rid), Some(eba::State::Pending));
}
