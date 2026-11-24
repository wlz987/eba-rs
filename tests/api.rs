//! 补齐低覆盖的公共原语：错误 Display、Effect kind、Context builder、Mailbox 边角。
use eba_rs::*;
#[test]
fn all_error_variants_display() {
    let errs = vec![
        EbaError::Bus("x".into()),
        EbaError::MailboxFull("x".into()),
        EbaError::InvalidTopic("x".into()),
        EbaError::AmbiguousRoute("x".into()),
        EbaError::Backpressure("x".into()),
        EbaError::Delivery("x".into()),
        EbaError::MailboxBusy("x".into()),
        EbaError::LeasePending("x".into()),
        EbaError::Wake("x".into()),
        EbaError::Clock("x".into()),
        EbaError::RecordSink("x".into()),
        EbaError::SeqExhausted("x".into()),
        EbaError::Closed("x".into()),
        EbaError::Reentrant("x".into()),
        EbaError::Other("x".into()),
    ];
    for e in errs {
        assert!(format!("{}", e).contains(':'));
    }
    assert_eq!(REENTRANT, "reentrant host execution");
}
#[test]
fn error_predicates_all() {
    assert!(EbaError::Clock("x".into()).is_clock());
    assert!(EbaError::RecordSink("x".into()).is_record_sink());
    assert!(EbaError::Backpressure("x".into()).is_backpressure());
    assert!(EbaError::MailboxBusy("x".into()).is_backpressure());
    assert!(EbaError::MailboxFull("x".into()).is_mailbox_full());
    assert!(EbaError::Clock("x".into()).is_clock());
}
#[test]
fn effect_kinds_and_to_array() {
    use eba_rs::job::effect::to_effect_array;
    let lease = Lease::new(LeaseOptions::default());
    let g = SeqIdGen::new();
    let intent = Intent {
        stage: "s".into(),
        request: make_envelope(MakeEnvelope {
            topic: "t".into(),
            payload: Value::Null,
            from: "a".into(),
            id_gen: &g,
            address: Address::Event,
            cause: None,
            sv: 0,
        })
        .unwrap(),
        timeout_ms: None,
        reply_to: "r".into(),
    };
    let effects = vec![
        Effect::Ask { intent },
        Effect::Reply { body: Value::Null },
        Effect::Hold {
            lease: lease.clone(),
        },
        Effect::Finish { body: Value::Null },
        Effect::Settle {
            lease: lease.clone(),
            body: ok_payload(Value::Null),
        },
        Effect::Revoke {
            lease: lease.clone(),
            error: None,
        },
        Effect::Tell {
            topic: "t".into(),
            payload: Value::Null,
        },
        Effect::Record {
            topic: "t".into(),
            payload: Value::Null,
        },
        Effect::Barrier { up_to: None },
        Effect::Timer { deadline_ms: 1 },
        Effect::Close { reason: None },
    ];
    let kinds: Vec<&str> = effects.iter().map(|e| e.kind()).collect();
    assert_eq!(
        kinds,
        vec![
            "ask", "reply", "hold", "finish", "settle", "revoke", "tell", "record", "barrier",
            "timer", "close"
        ]
    );
    assert!(to_effect_array(None).is_empty());
    assert_eq!(
        to_effect_array(Some(vec![Effect::Close { reason: None }])).len(),
        1
    );
}
#[test]
fn context_builders_all_effects() {
    let g = SeqIdGen::new();
    let root = make_envelope(MakeEnvelope {
        topic: "root".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 0,
    })
    .unwrap();
    let mut state = eba_rs::job::JobState::new(root, 100);
    let mut ctx = Context::new(&mut state, "actor", "reply", &g);
    // 纯 builder：返回 Effect，不落地。
    let ask = ctx.ask("s", "t", Value::Null, Some(5)).unwrap();
    assert!(matches!(ask, Effect::Ask { .. }));
    assert!(matches!(
        ctx.reply_effect(ok_payload(Value::Null)),
        Effect::Reply { .. }
    ));
    assert!(matches!(
        ctx.finish_effect(ok_payload(Value::Null)),
        Effect::Finish { .. }
    ));
    assert!(matches!(ctx.tell("t", Value::Null), Effect::Tell { .. }));
    assert!(matches!(
        ctx.record("t", Value::Null),
        Effect::Record { .. }
    ));
    assert!(matches!(ctx.barrier(None), Effect::Barrier { .. }));
    assert!(matches!(ctx.timer(5), Effect::Timer { .. }));
    assert!(matches!(ctx.close(None), Effect::Close { .. }));
    assert_eq!(ctx.actor_id(), "actor");
    assert_eq!(ctx.reply_to(), "reply");
    // request = ask + push
    let id = ctx.request("s", "t", Value::Null, None).unwrap();
    assert!(!id.is_empty());
    assert!(!ctx.take_effects().is_empty());
}
#[test]
fn context_hold_rules() {
    let g = SeqIdGen::new();
    let root = make_envelope(MakeEnvelope {
        topic: "root".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 0,
    })
    .unwrap();
    let mut state = eba_rs::job::JobState::new(root, 100);
    let mut ctx = Context::new(&mut state, "actor", "reply", &g);
    let l = ctx.hold(LeaseOptions::default()).unwrap();
    assert!(l.is_bound());
    // hold 后再 hold 应被拒绝（effects 非空）
    assert!(ctx.hold(LeaseOptions::default()).is_err());
}
#[test]
fn mailbox_drain_returns_parked() {
    let bus = Bus::new();
    let inbox = Inbox::new(0); // 容量 0 → 任何 publish 都 full
    bus.subscribe("t", parse_pattern("t").unwrap(), inbox);
    let mut mb = Mailbox::new(4);
    let e = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &SeqIdGen::new(),
        address: Address::Event,
        cause: None,
        sv: 0,
    })
    .unwrap();
    mb.send(&bus, e).unwrap();
    assert_eq!(mb.len(), 1);
    let drained = mb.drain();
    assert_eq!(drained.len(), 1);
    assert!(mb.is_empty());
}
#[test]
fn mailbox_bury_wrong_head_is_noop() {
    let mut mb = Mailbox::new(4);
    let g = SeqIdGen::new();
    let a = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 0,
    })
    .unwrap();
    let b = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "b".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 0,
    })
    .unwrap();
    let bus = Bus::new();
    let inbox = Inbox::new(0);
    bus.subscribe("t", parse_pattern("t").unwrap(), inbox);
    mb.send(&bus, a.clone()).unwrap();
    mb.send(&bus, b.clone()).unwrap();
    // bury 非组头的 b 无效
    assert!(mb.bury(&b).is_none());
    assert_eq!(mb.len(), 2);
    // bury 组头 a 成功
    assert!(mb.bury(&a).is_some());
    assert_eq!(mb.len(), 1);
}
#[test]
fn all_twelve_error_classes_have_variants() {
    // 的 12 个具名错误类在 Rust 中一一映射为 EbaError 的变体。
    let classes = vec![
        EbaError::Bus("x".into()),
        EbaError::MailboxFull("x".into()),
        EbaError::InvalidTopic("x".into()),
        EbaError::AmbiguousRoute("x".into()),
        EbaError::Backpressure("x".into()),
        EbaError::Delivery("x".into()),
        EbaError::MailboxBusy("x".into()),
        EbaError::LeasePending("x".into()),
        EbaError::Wake("x".into()),
        EbaError::Clock("x".into()),
        EbaError::RecordSink("x".into()),
        EbaError::SeqExhausted("x".into()),
    ];
    assert_eq!(classes.len(), 12);
    for e in classes {
        assert!(!format!("{}", e).is_empty());
    }
}
#[test]
fn mailbox_flush_one_checked_then_advanced() {
    let bus = Bus::new();
    let inbox = Inbox::new(1);
    bus.subscribe("t", parse_pattern("t").unwrap(), inbox.clone());
    let mut mb = Mailbox::new(4);
    let g = SeqIdGen::new();
    let a = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 0,
    })
    .unwrap();
    let b = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "b".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 0,
    })
    .unwrap();
    // 占满目标
    bus.publish(&a).unwrap();
    // park 两封到同 topic
    mb.send(&bus, b.clone()).unwrap();
    // 腾空一格：flush_one 应投递成功并标记为 Advanced
    {
        let r = inbox.receiver();
        r.borrow();
        r.try_recv();
        r.unborrow();
    }
    let (step, sent) = mb.flush_one(&bus);
    assert_eq!(
        step,
        Step::Advanced,
        "parked letter delivers once space frees"
    );
    assert_eq!(sent.as_ref().map(|e| &e.header.id), Some(&b.header.id));
    assert!(mb.is_empty(), "only b was parked");
    // mailbox 空后再 flush 是 Idle
    assert_eq!(mb.flush_one(&bus).0, Step::Idle);
}
#[test]
fn mailbox_no_candidate_flush_is_idle() {
    let bus = Bus::new();
    let mut mb = Mailbox::new(4);
    assert_eq!(mb.flush_one(&bus).0, Step::Idle);
}
#[test]
fn all_named_error_types_are_reachable() {
    // 这些名字在 是错误类；Rust 以 EbaError 别名暴露，确保调用方可按名字引用。
    let _: Option<BusError> = None;
    let _: Option<MailboxFull> = None;
    let _: Option<InvalidTopic> = None;
    let _: Option<AmbiguousRoute> = None;
    let _: Option<BackpressureError> = None;
    let _: Option<DeliveryError> = None;
    let _: Option<MailboxBusy> = None;
    let _: Option<LeasePending> = None;
    let _: Option<WakeError> = None;
    let _: Option<ClockError> = None;
    let _: Option<RecordSinkError> = None;
    let _: Option<SeqExhausted> = None;
    let _: Option<PoolClosedError> = None;
    let _: Option<PoolQueueFullError> = None;
    let _: Option<PoolAbortedError> = None;
}
