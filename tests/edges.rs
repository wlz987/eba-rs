//! 补齐 notify、pattern 边角、hook 默认实现、Call 取消。
use eba_rs::*;
use std::cell::RefCell;
use std::rc::Rc;
#[test]
fn watchers_on_empty_hook_fires() {
    let fired = Rc::new(RefCell::new(0));
    let f = fired.clone();
    let w = Watchers::new(1, Some(Box::new(move || *f.borrow_mut() += 1)));
    let sig = Rc::new(Signal::new());
    let interest = Rc::new(Interest::new(1, "t", sig));
    w.watch(&interest);
    w.remove(&interest);
    assert_eq!(
        *fired.borrow(),
        1,
        "onEmpty fires when last interest removed"
    );
}
#[test]
fn interest_owner_tracking() {
    let sig = Rc::new(Signal::new());
    let interest = Rc::new(Interest::new(1, "t", sig));
    let w1 = Watchers::new(1, None);
    let w2 = Watchers::new(2, None);
    w1.watch(&interest);
    w2.watch(&interest);
    assert_eq!(interest.owners_len(), 2);
    w1.remove(&interest);
    assert_eq!(interest.owners_len(), 1);
}
#[test]
fn signal_rebind_to_new_owner() {
    let s = Signal::new();
    s.bind(1, "a");
    s.bind(2, "b");
    assert!(s.bound_to(2));
    assert!(!s.bound_to(1));
    assert_eq!(s.actor().as_deref(), Some("b"));
}
#[test]
fn pattern_globstar_and_star_edges() {
    // 通配符只在末段生效；中间 `*` / `**` 按字面量处理。
    let p = parse_pattern("a.*.c").unwrap();
    assert_eq!(p.wildcard, Wildcard::None);
    assert!(p.matches("a.*.c"));
    assert!(!p.matches("a.b.c"));
    let s = parse_pattern("a.*").unwrap();
    assert!(s.matches("a.b"));
    assert!(!s.matches("a.b.c"));
    let g = parse_pattern("**").unwrap();
    assert!(g.matches("anything.at.all"));
    // 非法 topic
    assert!(split_topic("").is_err());
    assert!(split_topic("A.b").is_err());
    assert!(split_topic("a..b").is_err());
    // 中间 ** 不再是错误
    assert_eq!(parse_pattern("a.**.b").unwrap().wildcard, Wildcard::None);
}
#[test]
fn default_hook_implementations_are_noop() {
    struct Bare;
    impl JobHooks for Bare {}
    let g = SeqIdGen::new();
    let root = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 0,
    })
    .unwrap();
    let mut state = eba_rs::job::JobState::new(root, 100);
    let h = Bare;
    let mut ctx = Context::new(&mut state, "a", "r", &g);
    assert!(h.begin(&mut ctx).is_empty());
    let c = Completion {
        request_id: "r".into(),
        body: None,
    };
    assert!(h.on_stage_result(&mut ctx, "s", c).is_empty());
    assert!(h.on_resume(&mut ctx).is_empty());
    assert!(h.on_finished(None).is_empty());
    assert!(!state.finished());
}
#[test]
fn call_cancel_flag_and_slot() {
    let slot = CompletionSlot::new();
    let call = Call::new("t", Value::Null, None, slot.clone());
    assert!(!call.cancelled.get());
    call.cancelled.set(true);
    assert!(call.cancelled.get());
    call.settle(Completion {
        request_id: "r".into(),
        body: Some(err_payload("cancelled")),
    });
    assert!(call.slot.peek().is_some());
    *call.request_id.borrow_mut() = Some("req".into());
    assert_eq!(call.request_id.borrow().as_deref(), Some("req"));
}
#[test]
fn envelope_with_payload_and_sv() {
    let g = SeqIdGen::new();
    let e = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 1,
    })
    .unwrap();
    let e2 = e.clone().with_payload(Value::Int(5)).with_schema_version(3);
    assert_eq!(e2.payload, Value::Int(5));
    assert_eq!(e2.header.sv, 3);
    assert_eq!(e.header.sv, 1, "original unchanged");
}
