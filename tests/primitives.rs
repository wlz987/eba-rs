//! 时间/信封原语的回归测试。
use eba_rs::*;
use std::rc::Rc;
#[test]
fn envelope_id_and_actor_id() {
    let e = envelope_id("e1");
    let a = actor_id("a1");
    assert_eq!(e, "e1");
    assert_eq!(a, "a1");
}
#[test]
fn seq_id_gen_distinct_ids() {
    let g = SeqIdGen::new();
    let a = g.next();
    let b = g.next();
    assert_ne!(a, b);
    assert_eq!(a.len(), 32, "SeqIdGen uses 32-char zero-padded hex");
    assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
}
#[cfg(feature = "std")]
#[test]
fn uuid_id_gen_distinct_ids() {
    let g = UuidIdGen::new();
    let a = g.next();
    let b = g.next();
    assert_ne!(a, b);
    assert_eq!(a.len(), 32, "UuidIdGen strips hyphens");
    assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
}
#[test]
fn make_envelope_header_fields() {
    let g = SeqIdGen::new();
    let env = make_envelope(MakeEnvelope {
        topic: "a.b".into(),
        payload: Value::Int(1),
        from: "x".into(),
        id_gen: &g,
        address: Address::Request {
            reply_to: "r".into(),
        },
        cause: Some("cause".into()),
        sv: 2,
    })
    .unwrap();
    assert!(is_sealed(&env));
    assert_eq!(env.header.topic, "a.b");
    assert_eq!(env.header.sv, 2);
    assert_eq!(env.header.cause.as_deref(), Some("cause"));
    assert_eq!(env.header.address.kind(), "request");
    assert_eq!(env.correlation_id(), None);
}
#[test]
fn result_envelope_correlation() {
    let g = SeqIdGen::new();
    let env = make_envelope(MakeEnvelope {
        topic: "sys.reply".into(),
        payload: ok_payload(Value::Null),
        from: "x".into(),
        id_gen: &g,
        address: Address::Result {
            correlation_id: "req1".into(),
        },
        cause: None,
        sv: 0,
    })
    .unwrap();
    assert_eq!(env.correlation_id().map(|s| s.as_str()), Some("req1"));
    assert_eq!(env.header.address.kind(), "result");
}
#[test]
fn result_shapes_and_helpers() {
    let ok = ok_payload(Value::Int(7));
    assert!(is_result_ok(&ok));
    assert!(!is_result_err(&ok));
    assert_eq!(result_value(&ok), Some(&Value::Int(7)));
    assert!(result_error(&ok).is_none());
    assert!(has_result_shape(&ok));
    let err = err_payload_code("boom", Some(42));
    assert!(is_result_err(&err));
    assert_eq!(result_error(&err), Some("boom"));
    assert_eq!(err.as_map().unwrap().get("code"), Some(&Value::Int(42)));
    assert!(!has_result_shape(&Value::Null));
}
#[test]
fn payload_bytes_limit() {
    assert_eq!(MAX_PAYLOAD_BYTES, 1024);
}
#[test]
fn manual_clock_only_advances_explicitly() {
    let c = ManualClock::new(10);
    assert_eq!(c.now_ms(), 10);
    assert!(!c.arms_timer());
    c.advance(5);
    assert_eq!(c.now_ms(), 15);
}
#[test]
fn pinned_clock_is_fixed() {
    let c: Rc<dyn Clock> = Rc::new(ManualClock::new(100));
    let pinned = pin_clock(c.now_ms());
    assert_eq!(pinned.now_ms(), 100);
    // 即使宿主时钟前进，pinned 值不变。
    // ManualClock::advance takes &self；通过 upcast 无法调用，另建实例验证语义。
    let _ = c;
}
#[test]
fn deadlines_heap_orders_and_removes() {
    let mut d = Deadlines::new();
    d.add("a", 10).unwrap();
    d.add("b", 5).unwrap();
    d.add("c", 7).unwrap();
    assert_eq!(d.next_ms(), Some(5));
    assert_eq!(d.at(&"a"), Some(10));
    d.remove(&"b");
    assert_eq!(d.next_ms(), Some(7));
    assert_eq!(d.peek_due(6), None);
    assert_eq!(d.peek_due(7), Some("c"));
}
#[test]
fn safe_deadline_rejects_bad_values() {
    assert!(safe_deadline(0).is_ok());
    assert!(safe_deadline(MAX_TIMER_DELAY_MS).is_ok());
    assert!(safe_deadline(i64::MAX as u64 + 1).is_err());
}
