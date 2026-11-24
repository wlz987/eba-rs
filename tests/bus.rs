//! Bus 路由/trust/claim/tell。
use eba_rs::*;
use std::rc::Rc;
fn env(topic: &str, kind: Address, from: &str) -> Envelope {
    make_envelope(MakeEnvelope {
        topic: topic.into(),
        payload: ok_payload(Value::Int(1)),
        from: from.into(),
        id_gen: &SeqIdGen::new(),
        address: kind,
        cause: None,
        sv: 0,
    })
    .unwrap()
}
#[test]
fn bus_publish_no_route_is_retryable_miss_event_fans_out() {
    let bus = Bus::new();
    let e = env("nobody", Address::Event, "a");
    assert!(!bus.publish(&e).unwrap(), "no route = false, not error");
    let i1 = Inbox::new(8);
    let i2 = Inbox::new(8);
    bus.subscribe("one", parse_pattern("fan").unwrap(), i1.clone());
    bus.subscribe("two", parse_pattern("fan").unwrap(), i2.clone());
    let ev = env("fan", Address::Event, "a");
    assert!(bus.publish(&ev).unwrap());
    assert_eq!(i1.remaining(), 7);
    assert_eq!(i2.remaining(), 7);
}
#[test]
fn bus_request_must_be_unambiguous() {
    let bus = Bus::new();
    bus.subscribe("a", parse_pattern("dup").unwrap(), Inbox::new(8));
    bus.subscribe("b", parse_pattern("dup").unwrap(), Inbox::new(8));
    let req = env(
        "dup",
        Address::Request {
            reply_to: "r".into(),
        },
        "a",
    );
    assert!(matches!(
        bus.publish(&req),
        Err(EbaError::AmbiguousRoute(_))
    ));
}
#[test]
fn bus_full_inbox_rejects_publish_tell_skips() {
    let bus = Bus::new();
    let inbox = Inbox::new(1);
    bus.subscribe("t", parse_pattern("t").unwrap(), inbox.clone());
    let a = env("t", Address::Event, "a");
    let b = env("t", Address::Event, "b");
    assert!(bus.publish(&a).unwrap());
    assert!(matches!(bus.publish(&b), Err(EbaError::MailboxFull(_))));
    // tell：满则跳过并计入 dropped
    let dropped = bus.tell(&b).unwrap();
    assert_eq!(dropped, 1);
}
#[test]
fn bus_trust_policy_rejects_untrusted() {
    let bus = Bus::new();
    bus.subscribe("t", parse_pattern("t").unwrap(), Inbox::new(8));
    bus.set_trust_policy(Some(TrustPolicy::new(Box::new(|from| from == "trusted"))));
    let good = env("t", Address::Event, "trusted");
    let bad = env("t", Address::Event, "evil");
    assert!(bus.publish(&good).is_ok());
    assert!(bus.publish(&bad).is_err());
    bus.set_trust_policy(None);
    assert!(bus.publish(&bad).is_ok());
}
#[test]
fn bus_dedup_same_id_lands_once() {
    let bus = Bus::new();
    let inbox = Inbox::new(8);
    bus.subscribe("t", parse_pattern("t").unwrap(), inbox.clone());
    let g = SeqIdGen::new();
    let spec = MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 0,
    };
    let e1 = make_envelope(MakeEnvelope {
        topic: spec.topic.clone(),
        payload: spec.payload.clone(),
        from: spec.from.clone(),
        id_gen: spec.id_gen,
        address: spec.address.clone(),
        cause: None,
        sv: 0,
    })
    .unwrap();
    let e2 = e1.clone();
    assert!(bus.publish(&e1).unwrap());
    assert!(!bus.publish(&e2).unwrap(), "dedup by (from,id)");
}
#[test]
fn bus_claim_is_single_writer() {
    let bus = Bus::new();
    let token = bus.claim(1).unwrap();
    assert!(bus.claim(2).is_err(), "bus already claimed");
    assert_eq!(bus.owner(), Some(1));
    bus.release(token);
    assert_eq!(bus.owner(), None);
    // release 后重新可 claim
    assert!(bus.claim(3).is_ok());
}
#[test]
fn runtime_duplicate_host_and_admission_after_close() {
    let mut rt = Runtime::new(
        Bus::new(),
        Rc::new(ManualClock::new(0)),
        Rc::new(SeqIdGen::new()),
    )
    .unwrap();
    let host = || JobHost {
        actor_id: "h".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("t").unwrap()],
        make_job: Box::new(|_| Box::new(FinishJob(ok_payload(Value::Null)))),
    };
    rt.add(host()).unwrap();
    assert!(rt.add(host()).is_err(), "duplicate actor");
    rt.close(None);
    assert!(rt.add(host()).is_err(), "admission after close");
}
#[test]
fn runtime_ask_with_callback() {
    let mut rt = Runtime::new(
        Bus::new(),
        Rc::new(ManualClock::new(0)),
        Rc::new(SeqIdGen::new()),
    )
    .unwrap();
    rt.add(JobHost {
        actor_id: "h".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("t").unwrap()],
        make_job: Box::new(|_| Box::new(FinishJob(Value::Int(9)))),
    })
    .unwrap();
    let fired = Rc::new(std::cell::RefCell::new(None));
    let f = fired.clone();
    rt.ask_with("t", Value::Null, "caller", None, move |c| {
        *f.borrow_mut() = Some(c);
    });
    // pay 回调必须在结算时触发一次（不是同步立即触发）。
    assert!(
        fired.borrow().is_none(),
        "callback fires at settlement, not before"
    );
    for _ in 0..10 {
        rt.advance().unwrap();
        if fired.borrow().is_some() {
            break;
        }
    }
    let c = fired
        .borrow()
        .clone()
        .expect("pay callback should have fired");
    assert_eq!(result_value(c.body.as_ref().unwrap()), Some(&Value::Int(9)));
}
#[test]
fn ask_from_is_used_as_request_from() {
    // from 自填：业务请求应带调用方给的 from（供 trust gate 判定）。
    use std::cell::RefCell;
    let seen = Rc::new(RefCell::new(None));
    struct Capture(Rc<RefCell<Option<String>>>);
    impl JobHooks for Capture {
        fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
            *self.0.borrow_mut() = Some(ctx.root().header.from.clone());
            ctx.finish(ok_payload(Value::Null));
            Vec::new()
        }
    }
    let s = seen.clone();
    let mut rt = Runtime::new(
        Bus::new(),
        Rc::new(ManualClock::new(0)),
        Rc::new(SeqIdGen::new()),
    )
    .unwrap();
    rt.add(JobHost {
        actor_id: "cap".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("cap").unwrap()],
        make_job: Box::new(move |_| Box::new(Capture(s.clone()))),
    })
    .unwrap();
    let p = rt.ask("cap", Value::Null, "alice", None);
    for _ in 0..10 {
        rt.advance().unwrap();
        if p.peek().is_some() {
            break;
        }
    }
    assert_eq!(seen.borrow().as_deref(), Some("alice"));
}
