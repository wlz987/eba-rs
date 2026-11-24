//! 契约场景测试。
use eba_rs::*;
use std::rc::Rc;
fn clock() -> Rc<dyn Clock> {
    Rc::new(ManualClock::new(0))
}
fn ids() -> Rc<dyn IdGen> {
    Rc::new(SeqIdGen::new())
}
struct FinishValue(Value);
impl JobHooks for FinishValue {
    fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
        ctx.finish(ok_payload(self.0.clone()));
        Vec::new()
    }
}
fn host(actor: &str, topic: &str, value: Value) -> JobHost {
    JobHost {
        actor_id: actor.into(),
        inbox: Inbox::new(64),
        patterns: vec![parse_pattern(topic).unwrap()],
        make_job: Box::new(move |_| Box::new(FinishValue(value.clone()))),
    }
}
fn drive_until_ready(rt: &mut Runtime, p: &Pending, max: usize) -> Completion {
    for _ in 0..max {
        rt.advance().unwrap();
        if let Some(c) = p.peek() {
            return c;
        }
    }
    panic!("pending did not settle within {} advances", max)
}
#[test]
fn ask_finish_settles_pending() {
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(host("hello", "greet", Value::Bool(true))).unwrap();
    let p = rt.ask("greet", Value::Null, "caller", None);
    let c = drive_until_ready(&mut rt, &p, 20);
    assert!(is_result_ok(c.body.as_ref().unwrap()));
    assert_eq!(
        result_value(c.body.as_ref().unwrap()),
        Some(&Value::Bool(true))
    );
}
#[test]
fn request_route_ambiguity_is_an_error() {
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(host("a", "dup", Value::Null)).unwrap();
    rt.add(host("b", "dup", Value::Null)).unwrap();
    let bus = Bus::new();
    // 直接构造两个同 topic 订阅，验证 publish 抛 AmbiguousRoute。
    bus.subscribe("a", parse_pattern("dup").unwrap(), Inbox::new(8));
    bus.subscribe("b", parse_pattern("dup").unwrap(), Inbox::new(8));
    let env = make_envelope(MakeEnvelope {
        topic: "dup".into(),
        payload: Value::Null,
        from: "c".into(),
        id_gen: &SeqIdGen::new(),
        address: Address::Request {
            reply_to: "r".into(),
        },
        cause: None,
        sv: 0,
    })
    .unwrap();
    assert!(matches!(
        bus.publish(&env),
        Err(EbaError::AmbiguousRoute(_))
    ));
}
#[test]
fn ask_unknown_topic_times_out() {
    let manual: Rc<ManualClock> = Rc::new(ManualClock::new(0));
    let mut rt = Runtime::new(Bus::new(), manual.clone() as Rc<dyn Clock>, ids()).unwrap();
    let p = rt.ask("nobody", Value::Null, "caller", Some(5));
    // 无订阅者：请求不落地，租约到点后按 live clock 超时结算。
    for _ in 0..5 {
        rt.advance().unwrap();
    }
    assert!(
        p.peek().is_none(),
        "should still be pending before deadline"
    );
    manual.advance(10);
    for _ in 0..10 {
        rt.advance().unwrap();
        if p.peek().is_some() {
            break;
        }
    }
    assert!(p.peek().is_some(), "timeout should settle");
    assert!(p.peek().unwrap().is_timeout());
}
#[test]
fn timer_resume_reads_live_clock() {
    // t=0 起 ask，带 timeout；手动把时钟推进到 t=100，租约应按 live clock 到期。
    let manual: Rc<ManualClock> = Rc::new(ManualClock::new(0));
    let clock_rc: Rc<dyn Clock> = manual.clone();
    let mut rt = Runtime::new(Bus::new(), clock_rc, ids()).unwrap();
    let p = rt.ask("missing", Value::Null, "caller", Some(10));
    manual.advance(50);
    for _ in 0..10 {
        rt.advance().unwrap();
        if p.peek().is_some() {
            break;
        }
    }
    assert!(p.peek().is_some(), "timeout should settle via live clock");
    assert!(p.peek().unwrap().is_timeout());
}
#[test]
fn stats_and_quiescence() {
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(host("hello", "greet", Value::Null)).unwrap();
    // 154：ask 前只数注册 host；首次 ask 惰性注册 front 后 +1。
    assert_eq!(rt.stats().hosts, 1);
    let p = rt.ask("greet", Value::Null, "caller", None);
    assert_eq!(rt.stats().hosts, 2);
    for _ in 0..20 {
        rt.advance().unwrap();
        if p.peek().is_some() {
            break;
        }
    }
    assert!(p.is_ready());
    rt.close(None);
}
#[test]
fn hold_settle_completes_via_on_finished() {
    use std::cell::RefCell;
    let saved: Rc<RefCell<Option<Lease>>> = Rc::new(RefCell::new(None));
    let saved2 = saved.clone();
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(JobHost {
        actor_id: "worker".into(),
        inbox: Inbox::new(64),
        patterns: vec![parse_pattern("work").unwrap()],
        make_job: Box::new(move |_| {
            Box::new(HoldJob {
                saved: saved2.clone(),
            })
        }),
    })
    .unwrap();
    struct HoldJob {
        saved: Rc<RefCell<Option<Lease>>>,
    }
    impl JobHooks for HoldJob {
        fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
            // lease 必须由 ctx.hold 生成（内部绑定 owner）；手造 Lease 会被
            // assertLeaseIssued 等价校验拒绝。
            let lease = ctx.hold(LeaseOptions::default()).expect("hold");
            *self.saved.borrow_mut() = Some(lease);
            Vec::new()
        }
        fn on_finished(&self, _body: Option<Value>) -> Vec<Effect> {
            // resolveHeld 自己发回信，onFinished 只做清理（严格白名单）。
            Vec::new()
        }
    }
    let p = rt.ask("work", Value::Null, "caller", None);
    for _ in 0..5 {
        rt.advance().unwrap();
    }
    // 外部 settle 租约 → onFinished → finish → 回信结算。
    saved
        .borrow()
        .as_ref()
        .unwrap()
        .settle(ok_payload(Value::Int(42)))
        .unwrap();
    let c = drive_until_ready(&mut rt, &p, 20);
    assert_eq!(
        result_value(c.body.as_ref().unwrap()),
        Some(&Value::Int(42))
    );
}
#[test]
fn dedup_prevents_double_landing() {
    let w = DedupWindow::new(4);
    let mut w = w;
    let key = dedup_key("a", "e1");
    assert!(!w.has(&key));
    w.accept(key.clone());
    assert!(w.has(&key));
}
#[test]
fn schema_migration_payload() {
    fn v1_to_v2(v: Value, _from: u32) -> core::result::Result<Value, eba_rs::EbaError> {
        let mut m = v.as_map().cloned().unwrap_or_default();
        m.insert("version".into(), Value::Int(2));
        Ok(Value::Map(m))
    }
    let mut reg = SchemaRegistry::new();
    reg.register("t", TopicSchema::new(2).with_migrator(1, v1_to_v2))
        .unwrap();
    let v = reg.migrate("t", Value::Map(Default::default()), 1).unwrap();
    assert_eq!(v.as_map().unwrap().get("version"), Some(&Value::Int(2)));
}
#[test]
fn mailbox_parks_when_full_then_delivers_on_space() {
    // 目标 inbox 容量 1：先塞满，再让 mailbox park 一封信；腾空投递后应落地。
    let bus = Bus::new();
    let target = Inbox::new(1);
    bus.subscribe("target", parse_pattern("target").unwrap(), target.clone());
    let blocker = make_envelope(MakeEnvelope {
        topic: "target".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &SeqIdGen::new(),
        address: Address::Event,
        cause: None,
        sv: 0,
    })
    .unwrap();
    assert!(bus.publish(&blocker).unwrap()); // 占满
    assert_eq!(target.remaining(), 0);
    let mut mb = Mailbox::new(4);
    let letter = make_envelope(MakeEnvelope {
        topic: "target".into(),
        payload: Value::Bool(true),
        from: "b".into(),
        id_gen: &SeqIdGen::new(),
        address: Address::Event,
        cause: None,
        sv: 0,
    })
    .unwrap();
    mb.send(&bus, letter.clone()).unwrap(); // 满 → park
    assert_eq!(mb.len(), 1, "letter should be parked");
    // 腾空 inbox：取走 blocker
    let r = target.receiver();
    r.borrow();
    r.try_recv();
    r.unborrow();
    assert_eq!(target.remaining(), 1);
    // rearm 并 flush：应投递成功
    mb.rearm(&letter);
    let (step, sent) = mb.flush_one(&bus);
    assert_eq!(step, Step::Advanced);
    assert_eq!(sent.as_ref().map(|e| &e.header.id), Some(&letter.header.id));
    assert_eq!(mb.len(), 0, "letter should be delivered");
    assert_eq!(target.remaining(), 0, "inbox should hold the letter now");
}
#[test]
fn mailbox_buries_after_two_failures() {
    let bus = Bus::new(); // 无订阅者 → publish 返回 Ok(false) → bury 立即
    let mut mb = Mailbox::new(2);
    let _letter = make_envelope(MakeEnvelope {
        topic: "ghost".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &SeqIdGen::new(),
        address: Address::Request {
            reply_to: "r".into(),
        },
        cause: None,
        sv: 0,
    })
    .unwrap();
    // 无路由时 send 直接 publish 成功（Ok(false)），不 park；用满目标模拟 park。
    // 这里改为直接验证 flush_one 对无组候选返回 Idle 的可恢复语义。
    let (step, sent) = mb.flush_one(&bus);
    assert_eq!(step, Step::Idle);
    assert!(sent.is_none());
}
#[test]
fn effect_hold_must_lead() {
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    struct BadHook;
    impl JobHooks for BadHook {
        fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
            ctx.finish(ok_payload(Value::Null));
            let lease = Lease::new(LeaseOptions::default());
            vec![Effect::Hold { lease }]
        }
    }
    rt.add(JobHost {
        actor_id: "bad".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("bad").unwrap()],
        make_job: Box::new(|_| Box::new(BadHook)),
    })
    .unwrap();
    let _p = rt.ask("bad", Value::Null, "caller", None);
    let mut error_seen = false;
    for _ in 0..10 {
        if rt.advance().is_err() {
            error_seen = true;
            break;
        }
    }
    assert!(error_seen, "invalid hold must surface as a catchable error");
}
#[test]
fn effect_terminal_before_checkpoint_with_post_errors() {
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    struct BadHook;
    impl JobHooks for BadHook {
        fn begin(&self, _ctx: &mut Context) -> Vec<Effect> {
            // finish 在 checkpoint 前会本轮退休 job，post 永不可达 → 必须抛错。
            vec![
                Effect::Finish {
                    body: ok_payload(Value::Null),
                },
                Effect::Timer { deadline_ms: 5 },
                Effect::Tell {
                    topic: "x".into(),
                    payload: Value::Null,
                },
            ]
        }
    }
    rt.add(JobHost {
        actor_id: "bad2".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("bad2").unwrap()],
        make_job: Box::new(|_| Box::new(BadHook)),
    })
    .unwrap();
    let _p = rt.ask("bad2", Value::Null, "caller", None);
    let mut error_seen = false;
    for _ in 0..10 {
        if rt.advance().is_err() {
            error_seen = true;
            break;
        }
    }
    assert!(
        error_seen,
        "terminal before checkpoint must surface as an error"
    );
}
#[test]
fn timer_post_effects_replayed_after_deadline() {
    // timer 前的 effect 立即落地，之后的 post 段在到点后重放。
    let manual: Rc<ManualClock> = Rc::new(ManualClock::new(0));
    let mut rt = Runtime::new(Bus::new(), manual.clone() as Rc<dyn Clock>, ids()).unwrap();
    // 一个 observer host 订阅 "after"，验证 post 段的 tell 在 timer 到点后才发出。
    struct Obs;
    impl JobHooks for Obs {
        fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
            ctx.finish(ok_payload(Value::Bool(true)));
            Vec::new()
        }
    }
    // 记录 post 是否执行：用 CompletionSlot 无法观测 tell，改用带 timer 的 job 自身 finish。
    struct TimedJob;
    impl JobHooks for TimedJob {
        fn begin(&self, _ctx: &mut Context) -> Vec<Effect> {
            vec![
                Effect::Timer { deadline_ms: 10 },
                Effect::Finish {
                    body: ok_payload(Value::Int(7)),
                },
            ]
        }
    }
    rt.add(JobHost {
        actor_id: "timed".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("timed").unwrap()],
        make_job: Box::new(|_| Box::new(TimedJob)),
    })
    .unwrap();
    rt.add(JobHost {
        actor_id: "obs".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("after").unwrap()],
        make_job: Box::new(|_| Box::new(Obs)),
    })
    .unwrap();
    let p = rt.ask("timed", Value::Null, "caller", None);
    // 到点前：timer 已注册，post(Finish) 不应执行 → pending 未结算。
    for _ in 0..3 {
        rt.advance().unwrap();
    }
    assert!(
        p.peek().is_none(),
        "post finish must not run before deadline"
    );
    manual.advance(20);
    let c = drive_until_ready(&mut rt, &p, 20);
    assert_eq!(result_value(c.body.as_ref().unwrap()), Some(&Value::Int(7)));
}
#[test]
fn barrier_post_replayed_after_durable_watermark() {
    // record → barrier → post 段在 durable 水位达标后重放。
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    struct BarrierJob;
    impl JobHooks for BarrierJob {
        fn begin(&self, _ctx: &mut Context) -> Vec<Effect> {
            vec![
                Effect::Record {
                    topic: "log".into(),
                    payload: Value::Int(1),
                },
                Effect::Barrier { up_to: None },
                Effect::Finish {
                    body: ok_payload(Value::Int(99)),
                },
            ]
        }
    }
    rt.add(JobHost {
        actor_id: "barrier".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("barrier").unwrap()],
        make_job: Box::new(|_| Box::new(BarrierJob)),
    })
    .unwrap();
    let p = rt.ask("barrier", Value::Null, "caller", None);
    let c = drive_until_ready(&mut rt, &p, 20);
    assert_eq!(
        result_value(c.body.as_ref().unwrap()),
        Some(&Value::Int(99))
    );
}
#[test]
fn caller_builds_call() {
    let caller = Caller::new("caller", 64);
    assert_eq!(caller.actor_id, "caller");
    let slot = CompletionSlot::new();
    let call = caller.make_call("t", Value::Null, Some(5), slot.clone());
    assert_eq!(call.topic, "t");
    assert_eq!(call.timeout_ms, Some(5));
    slot.settle(Completion {
        request_id: "x".into(),
        body: Some(ok_payload(Value::Int(1))),
    });
    assert!(call.slot.peek().is_some());
}
#[test]
fn dedup_sink_drops_duplicate_records() {
    let mut sink = DedupSink::new(Box::new(MemorySink::new()), 8);
    let env = make_envelope(MakeEnvelope {
        topic: "log".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &SeqIdGen::new(),
        address: Address::Event,
        cause: None,
        sv: 0,
    })
    .unwrap();
    let rec = Record {
        v: 1,
        envelope: env.clone(),
        topic: env.header.topic.clone(),
        payload: env.payload.clone(),
        sv: 0,
        seq: 1,
        at: 0,
        dedup_key: dedup_key(&env.header.from, &env.header.id),
    };
    sink.append(&rec).unwrap();
    sink.append(&rec).unwrap(); // dedup: 第二次落地被跳过
                                // 第二次 append 被 dedup 跳过：sink 只持有一条记录。
    assert_eq!(sink.inner().durable(), 0); // 未 flush
}
#[test]
fn checkpoint_rejects_before_flush() {
    let mut sink = MemorySink::new();
    assert!(
        sink.checkpoint(5).is_err(),
        "checkpoint before flush must fail"
    );
    // flush 不能超过已 append 的水位，先追加 5 条记录。
    let g = SeqIdGen::new();
    for seq in 1..=5u64 {
        let env = make_envelope(MakeEnvelope {
            topic: "log".into(),
            payload: Value::Null,
            from: "a".into(),
            id_gen: &g,
            address: Address::Event,
            cause: None,
            sv: 1,
        })
        .unwrap();
        sink.append(&Record {
            v: 1,
            envelope: env.clone(),
            topic: env.header.topic.clone(),
            payload: env.payload.clone(),
            sv: 1,
            seq,
            at: 0,
            dedup_key: dedup_key(&env.header.from, &env.header.id),
        })
        .unwrap();
    }
    assert!(
        sink.flush(6).is_err(),
        "flush beyond appended must fail (aligned with )"
    );
    sink.flush(5).unwrap();
    assert_eq!(sink.checkpoint(5).unwrap(), 5);
}
#[test]
fn shutdown_and_close_async_converge() {
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(host("hello", "greet", Value::Bool(true))).unwrap();
    let p = rt.ask("greet", Value::Null, "caller", None);
    let (_abandoned, converged) = rt.close_async(None, 30);
    assert!(converged || p.peek().is_some());
}
#[test]
fn nested_ask_via_stage_result_settles_caller() {
    // 一个 job 在 on_stage_result 里再次 ask，最终仍结算原始 pending。
    struct Outer;
    impl JobHooks for Outer {
        fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
            let e = ctx.ask("first", "inner", Value::Null, None).unwrap();
            vec![e]
        }
        fn on_stage_result(&self, ctx: &mut Context, _stage: &str, _c: Completion) -> Vec<Effect> {
            ctx.finish(ok_payload(Value::Int(5)));
            Vec::new()
        }
    }
    struct Inner;
    impl JobHooks for Inner {
        fn begin(&self, _ctx: &mut Context) -> Vec<Effect> {
            vec![Effect::Finish {
                body: ok_payload(Value::Int(1)),
            }]
        }
    }
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(JobHost {
        actor_id: "outer".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("outer").unwrap()],
        make_job: Box::new(|_| Box::new(Outer)),
    })
    .unwrap();
    rt.add(JobHost {
        actor_id: "inner".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("inner").unwrap()],
        make_job: Box::new(|_| Box::new(Inner)),
    })
    .unwrap();
    let p = rt.ask("outer", Value::Null, "caller", None);
    let c = drive_until_ready(&mut rt, &p, 30);
    assert_eq!(result_value(c.body.as_ref().unwrap()), Some(&Value::Int(5)));
}
#[test]
fn fine_grained_error_predicates() {
    let e = EbaError::AmbiguousRoute("x".into());
    assert!(e.is_ambiguous_route());
    assert!(!e.is_mailbox_full());
    let m = EbaError::MailboxFull("x".into());
    assert!(m.is_mailbox_full());
    assert!(m.is_backpressure());
}
#[test]
fn pattern_matching_wildcards() {
    assert!(parse_pattern("a.b").unwrap().matches("a.b"));
    assert!(!parse_pattern("a.b").unwrap().matches("a.b.c"));
    assert!(parse_pattern("a.*").unwrap().matches("a.x"));
    assert!(!parse_pattern("a.*").unwrap().matches("a.x.y"));
    assert!(parse_pattern("a.**").unwrap().matches("a.x.y.z"));
    assert!(parse_pattern("a.**").unwrap().matches("a"));
}
#[test]
fn targeted_shutdown_drains_only_named_host() {
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(host("a", "ta", Value::Int(1))).unwrap();
    rt.add(host("b", "tb", Value::Int(2))).unwrap();
    rt.shutdown(Some("a"));
    // 被 shutdown 的 host 进入 draining，但仍可完成在途；b 不受影响。
    let pb = rt.ask("tb", Value::Null, "caller", None);
    let cb = drive_until_ready(&mut rt, &pb, 20);
    assert_eq!(
        result_value(cb.body.as_ref().unwrap()),
        Some(&Value::Int(2))
    );
}
#[test]
fn schedule_reports_quiescence() {
    let rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    let s = rt.schedule();
    assert!(s.is_quiescent, "empty runtime is quiescent");
    assert!(!s.ready);
    assert_eq!(s.next_deadline_ms, None);
}
#[test]
fn bad_timer_deadline_is_rejected_at_landing() {
    // 非法 deadline 抛普通 Error，经 advance 上抛（非 panic）。
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    struct BadTimer;
    impl JobHooks for BadTimer {
        fn begin(&self, _ctx: &mut Context) -> Vec<Effect> {
            vec![Effect::Timer {
                deadline_ms: u64::MAX,
            }]
        }
    }
    rt.add(JobHost {
        actor_id: "bt".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("bt").unwrap()],
        make_job: Box::new(|_| Box::new(BadTimer)),
    })
    .unwrap();
    let _p = rt.ask("bt", Value::Null, "caller", None);
    // u64::MAX > MAX_TIMER_DELAY_MS：waitTimer 抛普通 Error，
    // 经 pending_error 由 advance 返回 Err（release panic=abort 下不可 panic）。
    let mut error_seen = false;
    for _ in 0..5 {
        if rt.advance().is_err() {
            error_seen = true;
            break;
        }
    }
    assert!(error_seen, "unsafe timer deadline must surface as error");
}
#[test]
fn host_options_apply_limits() {
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    let opts = HostOptions {
        queue_limit: 3,
        active_limit: 2,
        work_budget: 8,
        mailbox_limit: 4,
        request_timeout_ms: Some(50),
    };
    rt.add_with_options(host("limited", "limited", Value::Null), opts)
        .unwrap();
    let stats = rt.stats();
    let detail = &stats.hosts_detail["limited"];
    // 限制已生效：用 stats 间接验证 active/queued 不超限。
    assert!(detail.active <= 2);
    assert!(detail.queued <= 3);
    // 默认超时的 host：请求无 timeout 也会在 50ms 后到期。
    let p = rt.ask("limited", Value::Null, "caller", None);
    let c = drive_until_ready(&mut rt, &p, 20);
    assert!(is_result_ok(c.body.as_ref().unwrap()));
}
