//! Lease/LeaseRegistry/Call。
use eba_rs::*;
fn result(error: bool) -> Value {
    if error {
        err_payload("x")
    } else {
        ok_payload(Value::Int(1))
    }
}
#[test]
fn lease_ownership_settle_abort_close() {
    let l = Lease::new(LeaseOptions::default());
    assert!(!l.is_bound());
    l.bind("owner1");
    assert!(l.is_bound_to("owner1"));
    assert!(!l.is_bound_to("owner2"));
    assert!(!l.is_settled());
    l.settle(result(false)).unwrap();
    assert!(l.is_settled());
    // 非消耗
    assert!(l.take().is_some());
    assert!(l.take().is_some(), "take is non-consuming");
    l.close();
    assert!(l.is_closed());
}
#[test]
fn lease_rejects_second_settle_and_bad_body() {
    let l = Lease::new(LeaseOptions::default());
    l.bind("o");
    l.settle(result(false)).unwrap();
    assert!(l.settle(result(false)).is_err(), "already settled");
    let l2 = Lease::new(LeaseOptions::default());
    assert!(l2.settle(Value::Null).is_err(), "invalid result shape");
}
#[test]
fn lease_deadline_expiry_and_budget() {
    let l = Lease::new(LeaseOptions {
        deadline_ms: Some(10),
        budget_ms: Some(5),
    });
    assert!(!l.expired(9));
    assert!(l.expired(10));
    // remaining_ms 是预算余量（budget-spent），与 deadline 无关。
    assert_eq!(l.remaining_ms(3), Some(5));
    assert!(l.consume(5));
    assert!(!l.consume(1), "budget exhausted");
    let no_deadline = Lease::new(LeaseOptions::default());
    assert!(!no_deadline.expired(99999));
    assert_eq!(no_deadline.remaining_ms(5), None);
}
#[test]
fn lease_cancel_and_revoke() {
    let l = Lease::new(LeaseOptions::default());
    l.bind("o");
    assert!(l.cancel("cancelled"));
    assert!(l.is_aborted());
    assert!(!l.cancel("again"), "already aborted");
    // 写 body → is_settled() 为 true；后续 settle 不改写。
    assert!(l.is_settled());
    l.settle(result(false)).unwrap();
    assert_eq!(result_error(&l.take().unwrap()), Some("cancelled"));
}
#[test]
fn lease_registry_submit_take_result() {
    use eba_rs::make_envelope;
    let mut reg = LeaseRegistry::new();
    let l = Lease::new(LeaseOptions::default());
    l.bind("owner1");
    reg.submit(
        "req1".into(),
        l,
        "owner1".into(),
        "reply",
        Some(100),
        "sys.reply",
        Some("req1".into()),
    )
    .unwrap();
    assert_eq!(reg.size(), 1);
    assert!(reg.holds(&"req1".into()));
    assert_eq!(reg.count(&"owner1".into()), 1);
    assert_eq!(reg.next_deadline_ms(), Some(100));
    let env = make_envelope(MakeEnvelope {
        topic: "sys.reply".into(),
        payload: result(false),
        from: "x".into(),
        id_gen: &SeqIdGen::new(),
        address: Address::Result {
            correlation_id: "req1".into(),
        },
        cause: Some("req1".into()),
        sv: 0,
    })
    .unwrap();
    let (owner, stage, completion) = reg.take_result(&env).unwrap().unwrap();
    assert_eq!(owner, "owner1");
    assert_eq!(stage, "reply");
    assert!(completion.body.is_some());
    assert_eq!(reg.size(), 0);
    assert_eq!(reg.count(&"owner1".into()), 0);
}
#[test]
fn lease_registry_expire_queues_cancel() {
    let mut reg = LeaseRegistry::new();
    let l = Lease::new(LeaseOptions {
        deadline_ms: Some(10),
        budget_ms: None,
    });
    l.bind("owner1");
    reg.submit(
        "req1".into(),
        l,
        "owner1".into(),
        "reply",
        Some(10),
        "sys.reply",
        None,
    )
    .unwrap();
    let (owner, _, completion) = reg.expire_due(10).unwrap();
    assert_eq!(owner, "owner1");
    assert!(completion.is_timeout());
    // 到期未答 → 排入取消队列（防下游 job 永生，I2 教训）
    assert_eq!(reg.take_cancelled(), vec!["req1"]);
}
#[test]
fn lease_registry_cancel_owner() {
    let mut reg = LeaseRegistry::new();
    let l = Lease::new(LeaseOptions::default());
    l.bind("owner1");
    reg.submit(
        "req1".into(),
        l,
        "owner1".into(),
        "reply",
        None,
        "sys.reply",
        None,
    )
    .unwrap();
    reg.cancel(&"owner1".into());
    assert_eq!(reg.size(), 0);
    assert_eq!(reg.take_cancelled(), vec!["req1"]);
}
#[test]
fn completion_slot_is_one_shot() {
    let slot = CompletionSlot::new();
    assert!(slot.peek().is_none());
    slot.settle(Completion {
        request_id: "r".into(),
        body: Some(result(false)),
    });
    assert!(slot.peek().is_some());
    slot.settle(Completion {
        request_id: "r2".into(),
        body: Some(result(true)),
    });
    // 第二次 settle 被忽略
    assert_eq!(slot.peek().unwrap().request_id, "r");
    assert!(slot.take().is_some());
    assert!(slot.take().is_none());
}
