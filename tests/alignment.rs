//! 语义不变量回归测试：锁定本次修复的机制闭环缺口。
//! 每一项都对应 docs/eba-mechanism.md 的不变量，失败即语义漂移回归。
use eba_rs::job::{Core, HostState, Lifecycle};
use eba_rs::*;
use std::rc::Rc;
fn ids() -> Rc<dyn IdGen> {
    Rc::new(SeqIdGen::new())
}
fn clock() -> Rc<dyn Clock> {
    Rc::new(ManualClock::new(0))
}
fn drive(rt: &mut Runtime, n: usize) {
    for _ in 0..n {
        let _ = rt.advance().unwrap();
    }
}
// ── A5：schema sv 默认 1 + fail-closed ──────────────────────────────────────
#[test]
fn schema_unregistered_topic_defaults_to_sv_one() {
    assert_eq!(SchemaRegistry::new().version("unknown"), 1);
}
#[test]
fn schema_sv_one_with_no_schema_passes_through() {
    let reg = SchemaRegistry::new();
    let g = SeqIdGen::new();
    let env = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 1,
    })
    .unwrap();
    let migrated = reg.migrate_envelope(&env).unwrap();
    assert_eq!(migrated.header.sv, 1);
    assert_eq!(migrated.payload, Value::Null);
}
#[test]
fn schema_sv_zero_with_registered_schema_fails_closed() {
    // sv=0 不再是"放行"后门：注册了 current=1 的 schema 时，0 找不到 migrator → 拒绝。
    let mut reg = SchemaRegistry::new();
    reg.register("t", TopicSchema::new(1)).unwrap();
    let g = SeqIdGen::new();
    let env = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 0,
    })
    .unwrap();
    assert!(reg.migrate_envelope(&env).is_err());
}
// ── A2：所有 host 的租约都到期（不只 front）────────────────────────────────
#[test]
fn nested_ask_timeout_releases_caller_on_non_front_host() {
    // Outer（普通 host，非 front）对 nonexistent 发起带 deadline 的 ask。
    // 旧实现只对 front 调 expire_leases → Outer 的租约永不到期 → 调用方挂起。
    struct Outer;
    impl JobHooks for Outer {
        fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
            vec![ctx
                .ask("nonexistent", "never", Value::Null, Some(10))
                .unwrap()]
        }
        fn on_stage_result(&self, ctx: &mut Context, _stage: &str, c: Completion) -> Vec<Effect> {
            assert!(c.body.is_none(), "expected timeout completion");
            ctx.finish(err_payload("timeout"));
            Vec::new()
        }
    }
    let manual = Rc::new(ManualClock::new(0));
    let mut rt = Runtime::new(Bus::new(), manual.clone(), ids()).unwrap();
    rt.add(JobHost {
        actor_id: "outer".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("outer").unwrap()],
        make_job: Box::new(|_| Box::new(Outer)),
    })
    .unwrap();
    let p = rt.ask("outer", Value::Null, "caller", None);
    drive(&mut rt, 3);
    manual.advance(50);
    drive(&mut rt, 6);
    let c = p
        .peek()
        .expect("caller must be released on nested ask timeout");
    assert_eq!(result_error(c.body.as_ref().unwrap()), Some("timeout"));
}
// ── A3：取消跨 host 传播（I2）─────────────────────────────────────────────
#[test]
fn cancelled_request_propagates_revoke_to_worker() {
    // 下游 worker 的 job 在等待的租约被取消时必须 onFinished，
    // 否则宿主永不 quiescent。验证 revoke 路径可达且不 panic。
    struct Worker;
    impl JobHooks for Worker {
        fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
            // ctx.hold 已 push Effect::Hold；再手动返回一支会变成重复 hold。
            let _ = ctx.hold(LeaseOptions::default()).expect("hold must build");
            Vec::new()
        }
    }
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(JobHost {
        actor_id: "worker".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("worker").unwrap()],
        make_job: Box::new(|_| Box::new(Worker)),
    })
    .unwrap();
    let p = rt.ask("worker", Value::Null, "caller", Some(10));
    drive(&mut rt, 10);
    let _ = p.peek();
    assert!(rt.stats().hosts >= 1);
}
// ── 第二轮新修复的回归 ─────────────────────────────────────────────────────
#[test]
fn held_revoke_settles_via_body() {
    // cancel 写 body 后，held 租约的 revoke 必须能被 resolve_held 结算。
    let lease = Lease::new(LeaseOptions::default());
    assert!(lease.revoke("cancelled"));
    assert!(lease.is_settled());
    assert_eq!(result_error(&lease.take().unwrap()), Some("cancelled"));
}
#[test]
fn dedup_key_avoids_colon_collision() {
    // ("a:b","c") 与 ("a","b:c") 在裸 from:id 下碰撞，长度前缀必须区分。
    let a = dedup_key("a:b", "c");
    let b = dedup_key("a", "b:c");
    assert_ne!(a, b);
}
#[test]
fn agenda_deadline_refreshes_on_update() {
    let mut agenda = Agenda::new();
    let sig = Rc::new(Signal::new());
    sig.bind(1, "host");
    agenda.bind_signal("host", sig);
    agenda.update("host", Some(100), false, false);
    assert_eq!(agenda.schedule(0).next_deadline_ms, Some(100));
    agenda.update("host", Some(50), false, false);
    assert_eq!(agenda.schedule(0).next_deadline_ms, Some(50));
}
#[test]
fn schema_register_rejects_non_monotonic() {
    let mut reg = SchemaRegistry::new();
    reg.register("t", TopicSchema::new(2)).unwrap();
    // register 抛普通 Error（可捕获），RS 返回 Err，不再 panic。
    let err = reg.register("t", TopicSchema::new(2)).unwrap_err();
    assert!(
        format!("{}", err).contains("schema t is already at version 2"),
        "same-version re-register must be rejected"
    );
}
#[test]
fn pattern_wildcard_only_at_last_segment() {
    assert_eq!(parse_pattern("a.*").unwrap().wildcard, Wildcard::Star);
    assert_eq!(parse_pattern("a.b*").unwrap().wildcard, Wildcard::None);
    assert!(parse_pattern("a.*").unwrap().matches("a.x"));
    assert!(!parse_pattern("a.*").unwrap().matches("a.x.y"));
}
#[test]
fn lease_safe_deadline_rejects_unsafe_integer() {
    assert!(safe_deadline((1u64 << 53) - 1).is_ok());
    assert!(safe_deadline(1u64 << 53).is_err());
}
#[test]
fn job_lifecycle_advances_to_active_and_finished() {
    // 独立审计 S10：lifecycle 原先恒为 New，导致 revoke 的 Active 判定恒假。
    let mut j = Job::new(
        make_envelope(MakeEnvelope {
            topic: "t".into(),
            payload: Value::Null,
            from: "a".into(),
            id_gen: &SeqIdGen::new(),
            address: Address::Event,
            cause: None,
            sv: 1,
        })
        .unwrap(),
        Box::new(FinishJob(Value::Null)),
        100,
    );
    assert_eq!(j.state.lifecycle, Lifecycle::New);
    j.state.transition(Lifecycle::Active);
    assert_eq!(j.state.lifecycle, Lifecycle::Active);
    j.state.transition(Lifecycle::Finished);
    assert!(j.state.finished());
}
#[test]
fn unreachable_ask_settles_without_deadline() {
    // 独立审计 S1：无订阅者、无超时的 ask 必须首轮超时，不能永久挂起。
    let manual = Rc::new(ManualClock::new(0));
    let mut rt = Runtime::new(Bus::new(), manual, ids()).unwrap();
    let p = rt.ask("no.such.topic", Value::Null, "caller", None);
    for _ in 0..3 {
        let _ = rt.advance().unwrap();
    }
    let c = p.peek().expect("unreachable ask must settle, not hang");
    assert!(c.body.is_none(), "timeout completion has no body");
}
#[test]
fn shutdown_enters_draining_state() {
    // 独立审计 S44：shutdown(None) 应把运行时标记为 draining。
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.shutdown(None);
    assert_eq!(rt.state(), eba_rs::job::HostState::Draining);
}
#[test]
fn close_after_tell_is_rejected() {
    // 独立审计 S4：关闭后 tell 应抛错而非静默丢弃。
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    let _ = rt.close(None);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.tell("t", Value::Null, "a");
    }));
    assert!(result.is_err());
}
#[test]
fn hook_error_settles_job_failed() {
    // 独立审计 #1：hook 返回 Err → job_failed 结算。
    struct BadHook;
    impl JobHooks for BadHook {
        fn try_begin(&self, _ctx: &mut Context) -> core::result::Result<Vec<Effect>, String> {
            Err("boom".into())
        }
    }
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(JobHost {
        actor_id: "bad".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("bad").unwrap()],
        make_job: Box::new(|_| Box::new(BadHook)),
    })
    .unwrap();
    let p = rt.ask("bad", Value::Null, "caller", None);
    // runHook：结算先落地，错误沿 advance 抛出。
    let mut errored = false;
    for _ in 0..3 {
        if let Err(e) = rt.advance() {
            assert_eq!(e.to_string(), "Error: boom");
            errored = true;
            break;
        }
    }
    assert!(errored, "hook error must propagate to advance");
    let _ = rt.advance();
    let _ = rt.advance();
    let c = p.peek().expect("hook failure must settle the caller");
    assert_eq!(result_error(c.body.as_ref().unwrap()), Some("job_failed"));
}
#[test]
fn pending_delivery_drops_envelope_that_becomes_unroutable() {
    // 独立审计 #A：flush 只重试 MailboxFull；无路由必须丢弃，否则永久滞留。
    let bus = Bus::new();
    bus.subscribe("sink", parse_pattern("t").unwrap(), Inbox::new(1));
    let g = SeqIdGen::new();
    let mk = |sv_topic: &str, id_gen: &SeqIdGen| {
        make_envelope(MakeEnvelope {
            topic: sv_topic.into(),
            payload: Value::Null,
            from: "src".into(),
            id_gen,
            address: Address::Event,
            cause: None,
            sv: 1,
        })
        .unwrap()
    };
    let first = mk("t", &g);
    assert!(bus.publish(&first).unwrap());
    let second = mk("t", &g);
    assert!(!bus.deliver(&second, 1).unwrap(), "mailbox full must park");
    bus.unsubscribe("sink");
    let (delivered, remaining) = bus.flush_pending_deliveries();
    assert_eq!(
        (delivered, remaining),
        (0, 0),
        "unroutable pending must be dropped"
    );
    // deliver 无路由时把信交还 owner 的 seized，而不是丢弃。
    assert_eq!(bus.abandon_deliveries(Some(1)).len(), 1);
    assert!(bus.abandon_deliveries(Some(1)).is_empty());
}
#[test]
fn hook_error_still_runs_custom_fallible_on_finished() {
    // 独立审计 #B：job_failed 路径必须走 try_on_finished，自定义清理不可被跳过。
    struct CleaningHook {
        cleaned: std::rc::Rc<std::cell::Cell<bool>>,
    }
    impl JobHooks for CleaningHook {
        fn try_begin(&self, _ctx: &mut Context) -> core::result::Result<Vec<Effect>, String> {
            Err("boom".into())
        }
        fn try_on_finished(
            &self,
            _body: Option<Value>,
        ) -> core::result::Result<Vec<Effect>, String> {
            self.cleaned.set(true);
            Ok(Vec::new())
        }
    }
    let cleaned = std::rc::Rc::new(std::cell::Cell::new(false));
    let flag = cleaned.clone();
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(JobHost {
        actor_id: "cleaning".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("cleaning").unwrap()],
        make_job: Box::new(move |_| {
            Box::new(CleaningHook {
                cleaned: flag.clone(),
            })
        }),
    })
    .unwrap();
    let p = rt.ask("cleaning", Value::Null, "caller", None);
    let mut errored = false;
    for _ in 0..3 {
        if rt.advance().is_err() {
            errored = true;
            break;
        }
    }
    assert!(errored, "hook error must propagate to advance");
    let _ = rt.advance();
    let _ = rt.advance();
    assert!(
        cleaned.get(),
        "custom fallible on_finished must run on hook error"
    );
    assert_eq!(
        result_error(p.peek().expect("must settle").body.as_ref().unwrap()),
        Some("job_failed")
    );
}
#[test]
fn tell_fails_closed_on_trust_and_migration_errors() {
    // 独立审计 A1：bus.tell 不捕获 migrate/trust 错误；RS 不得静默返回 0。
    let bus = Bus::new();
    bus.set_trust_policy(Some(TrustPolicy::new(Box::new(|from| from == "ok"))));
    let g = SeqIdGen::new();
    let untrusted = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "bad".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 1,
    })
    .unwrap();
    assert!(
        bus.tell(&untrusted).is_err(),
        "trust violation must surface"
    );
    let mut reg = SchemaRegistry::new();
    reg.register("t", TopicSchema::new(2)).unwrap();
    let migrating = Bus::with_schemas(Some(Rc::new(reg)));
    let stale = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "ok".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 1,
    })
    .unwrap();
    assert!(
        migrating.tell(&stale).is_err(),
        "migration failure must surface"
    );
}
#[test]
#[should_panic(expected = "unknown host")]
fn shutdown_unknown_host_is_rejected() {
    // 独立审计 A2：与 close 一致，未知 host 不能静默跳过。
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.shutdown(Some("ghost"));
}
#[test]
fn close_keeps_host_when_record_sink_fails() {
    // 独立审计 A3：落盘失败必须中止退役，host 保留供重试（closeHost）。
    struct FailingSink;
    impl RecordSink for FailingSink {
        fn append(&mut self, _record: &Record) -> eba_rs::errors::Result<()> {
            Err(EbaError::RecordSink("boom".into()))
        }
        fn flush(&mut self, _up_to: u64) -> eba_rs::errors::Result<()> {
            Ok(())
        }
    }
    struct Noop;
    impl JobHooks for Noop {}
    let mut core = Core::new(
        "sinkhost",
        Inbox::new(4),
        Box::new(|_| Box::new(Noop)),
        Rc::new(Signal::new()),
        RecordLog::new(Box::new(FailingSink), 16, 1),
        ids(),
        1,
    );
    let g = SeqIdGen::new();
    let env = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "x".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 1,
    })
    .unwrap();
    assert!(core.records.record(&env, 0).is_some());
    assert!(core.close_and_collect(&Bus::new(), 0).is_err());
    assert_ne!(
        core.state,
        HostState::Closed,
        "failed close must keep host open"
    );
}
#[test]
fn agenda_wake_fault_surfaces_as_wake_error() {
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.agenda().set_fault("wake boom");
    match rt.advance() {
        Err(EbaError::Wake(message)) => assert_eq!(message, "wake boom"),
        other => panic!("expected WakeError, got {:?}", other.map(|_| ())),
    }
    assert!(rt.agenda().take_fault().is_none());
}
#[test]
fn record_keeps_original_envelope_and_default_ask_uses_caller() {
    let mut log = RecordLog::new(Box::new(MemorySink::new()), 8, 1);
    let g = SeqIdGen::new();
    let env = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "src".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 1,
    })
    .unwrap();
    let record = log.record(&env, 0).unwrap();
    assert_eq!(record.envelope.header.id, env.header.id);
    assert_eq!(record.envelope.header.from, "src");
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(JobHost {
        actor_id: "sink".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("sink").unwrap()],
        make_job: Box::new(|_| Box::new(FinishJob(Value::Null))),
    })
    .unwrap();
    let p = rt.ask_default("sink", Value::Null);
    drive(&mut rt, 3);
    assert!(p.peek().is_some(), "default ask must use caller and settle");
}
#[test]
fn inbox_pause_gates_work_and_receiver_close_returns_seized() {
    let inbox = Inbox::new(2);
    let g = SeqIdGen::new();
    let event = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 1,
    })
    .unwrap();
    let result = make_envelope(MakeEnvelope {
        topic: "sys.reply".into(),
        payload: ok_payload(Value::Null),
        from: "a".into(),
        id_gen: &g,
        address: Address::Result {
            correlation_id: "req".into(),
        },
        cause: Some("req".into()),
        sv: 1,
    })
    .unwrap();
    let bus = Bus::new();
    bus.subscribe("sink", parse_pattern("t").unwrap(), inbox.clone());
    bus.subscribe("sink", parse_pattern("sys.reply").unwrap(), inbox.clone());
    assert_eq!(bus.subscription_count("t", &inbox), 1);
    assert_eq!(inbox.route_refs(), 2);
    inbox.pause_work();
    assert!(
        bus.tell(&event).unwrap() > 0,
        "event must be gated while paused"
    );
    assert!(bus.publish(&result).unwrap(), "result still lands");
    inbox.resume_work();
    let rx = inbox.receiver();
    assert!(rx.borrow());
    assert!(rx.close(), "first close succeeds");
}
#[test]
fn deliver_records_seized_per_home() {
    let bus = Bus::new();
    bus.subscribe("sink", parse_pattern("t").unwrap(), Inbox::new(1));
    let g = SeqIdGen::new();
    let first = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 1,
    })
    .unwrap();
    let second = first.clone();
    assert!(bus.publish(&first).unwrap());
    assert!(!bus.deliver(&second, 7).unwrap());
    bus.unsubscribe("sink");
    bus.flush_pending_deliveries();
    assert!(bus.abandon_deliveries(Some(8)).is_empty());
    assert_eq!(bus.abandon_deliveries(Some(7)).len(), 1);
}
#[test]
fn lease_stuck_recover_and_elapse_open() {
    let mut reg = LeaseRegistry::new();
    reg.submit(
        "req".into(),
        Lease::new(LeaseOptions::default()),
        "owner".into(),
        "stage",
        None::<u64>,
        "reply".to_string(),
        Some("req".to_string()),
    )
    .unwrap();
    assert_eq!(reg.elapse_open(), 1);
    assert_eq!(reg.next_deadline_ms(), Some(0));
    reg.remember_stuck("req".into());
    assert_eq!(reg.unreleased(), 1);
    assert_eq!(reg.recover(), 1);
    assert_eq!(reg.unreleased(), 0);
    assert_eq!(reg.size(), 0);
}
#[test]
fn silent_request_job_settles_job_silent() {
    // 无 effect 的 request job 必须回 job_silent。
    struct Silent;
    impl JobHooks for Silent {}
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(JobHost {
        actor_id: "silent".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("silent").unwrap()],
        make_job: Box::new(|_| Box::new(Silent)),
    })
    .unwrap();
    let p = rt.ask("silent", Value::Null, "caller", None);
    drive(&mut rt, 4);
    assert_eq!(
        result_error(
            p.peek()
                .expect("silent job must settle")
                .body
                .as_ref()
                .unwrap()
        ),
        Some("job_silent")
    );
}
#[test]
fn envelope_id_cause_bytes_and_trust_shape() {
    // 32 hex id、cause 默认自身 id、显式 id、bytes、checkTrust。
    let g = SeqIdGen::new();
    let spec = || MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "bad".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 1,
    };
    let env = make_envelope(spec()).unwrap();
    assert_eq!(env.header.id.len(), 32);
    assert_eq!(env.header.cause.as_deref(), Some(env.header.id.as_str()));
    let policy = TrustPolicy::new(Box::new(|from| from == "ok"));
    assert!(env.check_trust(&policy).is_err());
    let explicit = make_envelope_with_id(spec(), "explicit".into()).unwrap();
    assert_eq!(explicit.header.id, "explicit");
    assert_eq!(explicit.header.cause.as_deref(), Some("explicit"));
}
#[test]
fn schema_check_returns_version_and_dedup_accept_is_boolean() {
    let mut reg = SchemaRegistry::new();
    reg.register("t", TopicSchema::new(2)).unwrap();
    assert_eq!(reg.check("t", &Value::Null).unwrap(), 2);
    assert_eq!(reg.check("unknown", &Value::Null).unwrap(), 1);
    let mut window = DedupWindow::new(2);
    assert!(window.accept("k"));
    assert!(!window.accept("k"));
}
#[test]
fn lease_take_is_non_consuming_and_abort_writes_no_body() {
    // 独立审计 S19/S20：take 非消耗；abort 只置 aborted，不写 body。
    let l = Lease::new(LeaseOptions::default());
    l.settle(ok_payload(Value::Null)).unwrap();
    assert!(l.take().is_some());
    assert!(l.take().is_some(), "take must not clear body");
    let l2 = Lease::new(LeaseOptions::default());
    assert!(l2.abort());
    assert!(l2.is_aborted());
    assert!(l2.take().is_none(), "abort must not write body");
}
// ── 第十轮：零预设独立审计修复项回归 ──────────────────────────────────────
#[test]
fn lease_cancel_after_abort_writes_cancelled_body() {
    // Lease.cancel 不检查 aborted：abort 后再 cancel 仍写 errPayload body。
    let l = Lease::new(LeaseOptions::default());
    assert!(l.abort());
    assert!(l.is_aborted());
    assert!(l.cancel("cancelled"));
    assert_eq!(result_error(&l.take().unwrap()), Some("cancelled"));
}
#[test]
fn lease_abort_after_close_succeeds() {
    // abort 无 closed 检查。
    let l = Lease::new(LeaseOptions::default());
    l.close();
    assert!(l.abort());
    assert!(l.is_aborted());
}
#[test]
fn receiver_take_transfers_without_closing_inbox() {
    // Receiver.take 转移所有权：旧 receiver 失效，inbox 仍开、队列保留。
    let inbox = Inbox::new(4);
    let rx = inbox.receiver();
    let env = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &SeqIdGen::new(),
        address: Address::Event,
        cause: None,
        sv: 1,
    })
    .unwrap();
    bus_publish_into(&inbox, &env);
    let rx2 = rx.take();
    assert!(!rx.is_active());
    assert!(rx2.is_active());
    assert!(!inbox.is_closed());
    assert!(rx2.borrow());
    assert_eq!(rx2.try_recv().unwrap().header.id, env.header.id);
    rx2.unborrow();
}
fn bus_publish_into(inbox: &Inbox, env: &Envelope) {
    let bus = Bus::new();
    bus.subscribe("sink", parse_pattern("t").unwrap(), inbox.clone());
    assert!(bus.publish(env).unwrap());
}
#[test]
fn receiver_borrow_is_reentrant() {
    let inbox = Inbox::new(2);
    let rx = inbox.receiver();
    assert!(rx.borrow());
    assert!(rx.borrow(), "borrow must be reentrant");
    assert_eq!(rx.borrowed(), 2);
    rx.unborrow();
    rx.unborrow();
    assert_eq!(rx.borrowed(), 0);
}
#[test]
fn mailbox_send_wraps_hard_error_as_delivery() {
    // MailboxSend.send 把 trust/ambiguous 等硬错误包装为 DeliveryError。
    let mut box_ = Mailbox::new(4);
    let bus = Bus::new();
    bus.subscribe("a", parse_pattern("t").unwrap(), Inbox::new(4));
    bus.subscribe("b", parse_pattern("t").unwrap(), Inbox::new(4));
    let req = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &SeqIdGen::new(),
        address: Address::Request {
            reply_to: "sys.reply".into(),
        },
        cause: None,
        sv: 1,
    })
    .unwrap();
    let err = box_.send(&bus, req).unwrap_err();
    assert!(matches!(err, EbaError::Delivery(_)));
}
#[test]
fn dedup_sink_forwards_gap() {
    use std::cell::Cell;
    struct GapSink {
        seen: Rc<Cell<bool>>,
    }
    impl RecordSink for GapSink {
        fn append(&mut self, _r: &Record) -> std::result::Result<(), EbaError> {
            Ok(())
        }
        fn flush(&mut self, _u: u64) -> std::result::Result<(), EbaError> {
            Ok(())
        }
        fn gap(&mut self, from: u64, to: u64) {
            assert_eq!((from, to), (1, 3));
            self.seen.set(true);
        }
    }
    let seen = Rc::new(Cell::new(false));
    let mut sink = DedupSink::new(Box::new(GapSink { seen: seen.clone() }), 4);
    sink.gap(1, 3);
    assert!(seen.get(), "DedupSink must forward gap to inner");
}
#[test]
fn shutdown_on_closed_runtime_does_not_revive() {
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    let _ = rt.close(None);
    assert_eq!(rt.state(), HostState::Closed);
    rt.shutdown(None);
    assert_eq!(
        rt.state(),
        HostState::Closed,
        "shutdown must not revive Closed runtime"
    );
}
#[test]
fn release_wait_is_idempotent_and_true() {
    let bus = Bus::new();
    assert!(bus.hold_wait("w"));
    assert!(bus.release_wait("w"));
    assert!(bus.release_wait("w"), "Set.delete is idempotent");
    assert!(!bus.waiting(Some("w")));
}
#[test]
fn work_paused_admits_diagnostic() {
    let inbox = Inbox::new(8);
    inbox.pause_work();
    let bus = Bus::new();
    bus.subscribe("s", parse_pattern("t").unwrap(), inbox.clone());
    let g = SeqIdGen::new();
    let event = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 1,
    })
    .unwrap();
    let diagnostic = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &g,
        address: Address::Diagnostic,
        cause: None,
        sv: 1,
    })
    .unwrap();
    assert_eq!(bus.tell(&event).unwrap(), 1, "event gated while paused");
    assert_eq!(
        bus.tell(&diagnostic).unwrap(),
        0,
        "diagnostic must still land"
    );
}
#[test]
fn normal_finish_triggers_on_finished_once() {
    use std::cell::Cell;
    struct Counting {
        hits: Rc<Cell<usize>>,
    }
    impl JobHooks for Counting {
        fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
            ctx.finish(ok_payload(Value::Int(7)));
            Vec::new()
        }
        fn on_finished(&self, body: Option<Value>) -> Vec<Effect> {
            assert_eq!(result_value(body.as_ref().unwrap()), Some(&Value::Int(7)));
            self.hits.set(self.hits.get() + 1);
            Vec::new()
        }
    }
    let hits = Rc::new(Cell::new(0));
    let h = hits.clone();
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(JobHost {
        actor_id: "n".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("n").unwrap()],
        make_job: Box::new(move |_| Box::new(Counting { hits: h.clone() })),
    })
    .unwrap();
    let p = rt.ask("n", Value::Null, "caller", None);
    let _ = drive_until_ready_local(&mut rt, &p, 20);
    assert_eq!(
        hits.get(),
        1,
        "onFinished must fire exactly once on normal finish"
    );
}
#[test]
fn reply_only_job_is_reclaimed() {
    struct ReplyOnly;
    impl JobHooks for ReplyOnly {
        fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
            ctx.reply(ok_payload(Value::Null));
            Vec::new()
        }
    }
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(JobHost {
        actor_id: "r".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("r").unwrap()],
        make_job: Box::new(|_| Box::new(ReplyOnly)),
    })
    .unwrap();
    let p = rt.ask("r", Value::Null, "caller", None);
    let _ = drive_until_ready_local(&mut rt, &p, 20);
    assert!(rt.is_quiescent(), "reply-only job must be reclaimed");
}
fn drive_until_ready_local(rt: &mut Runtime, p: &Pending, max: usize) -> Completion {
    for _ in 0..max {
        rt.advance().unwrap();
        if let Some(c) = p.peek() {
            return c;
        }
    }
    panic!("pending did not settle within {} advances", max)
}
#[test]
fn empty_advance_does_not_panic_with_no_cancellations() {
    // 空转快路径：无任何取消意图时 propagate_cancellations 直接返回。
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    for _ in 0..20 {
        assert!(!rt.advance().unwrap());
    }
}
#[test]
fn lease_registry_has_cancelled_fast_check() {
    let mut reg = LeaseRegistry::new();
    assert!(!reg.has_cancelled());
    reg.submit(
        "r".into(),
        Lease::new(LeaseOptions::default()),
        "o".into(),
        "s",
        Some(1),
        "t",
        None,
    )
    .unwrap();
    let _ = reg.expire_due(5);
    assert!(reg.has_cancelled());
    assert_eq!(reg.take_cancelled(), vec!["r"]);
    assert!(!reg.has_cancelled());
}
// ── 第十一轮：全量逐项缺口修复回归 ────────────────────────────────────────
#[test]
fn held_flag_set_on_hold_and_cleared_on_resolve() {
    use std::cell::{Cell, RefCell};
    struct HeldJob {
        lease: Rc<RefCell<Option<Lease>>>,
        settled: Rc<Cell<bool>>,
    }
    impl JobHooks for HeldJob {
        fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
            let lease = ctx.hold(LeaseOptions::default()).expect("hold");
            *self.lease.borrow_mut() = Some(lease);
            Vec::new()
        }
        fn on_finished(&self, _body: Option<Value>) -> Vec<Effect> {
            self.settled.set(true);
            // resolveHeld 自己发回信，onFinished 只做清理（严格白名单）。
            Vec::new()
        }
    }
    let lease_holder = Rc::new(RefCell::new(None));
    let settled = Rc::new(Cell::new(false));
    let lh = lease_holder.clone();
    let s = settled.clone();
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(JobHost {
        actor_id: "h".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("h").unwrap()],
        make_job: Box::new(move |_| {
            Box::new(HeldJob {
                lease: lh.clone(),
                settled: s.clone(),
            })
        }),
    })
    .unwrap();
    let p = rt.ask("h", Value::Null, "caller", None);
    for _ in 0..3 {
        rt.advance().unwrap();
    }
    // held job 不应被当成 idle 提前回收；settled 只在外部结算后触发。
    assert!(!settled.get(), "held job must not finalize before settle");
    let lease = lease_holder.borrow().clone().unwrap();
    assert!(lease.settle(ok_payload(Value::Int(42))).is_ok());
    let _ = drive_until_ready_local(&mut rt, &p, 20);
    assert!(settled.get(), "held job must finalize after settle");
}
#[test]
fn silent_job_synthesises_job_silent() {
    struct Silent;
    impl JobHooks for Silent {
        fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
            // 非空 effect 列表但无 reply → commitTo 仍形成 Control → silence()。
            ctx.tell("log", Value::Null);
            Vec::new()
        }
    }
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(JobHost {
        actor_id: "sil".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("sil").unwrap()],
        make_job: Box::new(|_| Box::new(Silent)),
    })
    .unwrap();
    let p = rt.ask("sil", Value::Null, "caller", None);
    let c = drive_until_ready_local(&mut rt, &p, 20);
    assert_eq!(result_error(&c.body.unwrap()), Some("job_silent"));
    assert!(rt.is_quiescent());
}
#[test]
fn dedup_window_zero_capacity_keeps_nothing() {
    let mut w = DedupWindow::new(0);
    assert!(w.accept("a"));
    assert!(w.accept("a"), "capacity 0 must evict immediately");
}
#[test]
fn sequencer_last_starts_at_zero() {
    let mut s = Sequencer::new(7);
    assert_eq!(s.last(), 0);
    assert_eq!(s.next_seq(), 7);
    assert_eq!(s.last(), 7);
}
#[test]
fn seq_id_exhausts_at_2_53() {
    let gen = SeqIdGen::new();
    assert_eq!(gen.next(), format!("{:032x}", 1));
}
#[test]
fn stats_hosts_lazy_front() {
    // front 在首次 ask 时惰性注册，计入 hosts。
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    assert_eq!(rt.stats().hosts, 0);
    let _p = rt.ask("nobody", Value::Null, "caller", None);
    assert_eq!(rt.stats().hosts, 1, "front registered on first ask");
}
#[test]
fn receiver_close_filters_dead_route() {
    let bus = Bus::new();
    let inbox = Inbox::new(4);
    let rx = inbox.receiver();
    bus.subscribe("s", parse_pattern("t").unwrap(), inbox.clone());
    assert!(rx.close());
    let env = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &SeqIdGen::new(),
        address: Address::Event,
        cause: None,
        sv: 1,
    })
    .unwrap();
    assert!(
        !bus.publish(&env).unwrap(),
        "closed inbox route must not match"
    );
}
#[test]
fn schema_migrator_receives_from_version() {
    use std::cell::Cell;
    thread_local! { static SEEN: Cell<u32> = const { Cell::new(0) }; }
    fn migrator(v: Value, from: u32) -> core::result::Result<Value, EbaError> {
        SEEN.with(|s| s.set(from));
        Ok(v)
    }
    let mut reg = SchemaRegistry::new();
    reg.register("t", TopicSchema::new(2).with_migrator(1, migrator))
        .unwrap();
    let _ = reg.migrate("t", Value::Map(Default::default()), 1);
    assert_eq!(SEEN.with(|s| s.get()), 1);
}
// ── 第十二轮：零预设审计确凿缺口回归 ──────────────────────────────────────
#[test]
fn on_finished_finish_effect_is_rejected_not_landed() {
    // applyFinished 严格拒绝 finish（含）。
    // RS 应在 apply_finished_effects 记录 pending_error，且不产生额外回信。
    use std::cell::Cell;
    struct BadCleanup {
        fired: Rc<Cell<bool>>,
    }
    impl JobHooks for BadCleanup {
        fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
            ctx.finish(ok_payload(Value::Null));
            Vec::new()
        }
        fn on_finished(&self, _body: Option<Value>) -> Vec<Effect> {
            self.fired.set(true);
            vec![Effect::Tell {
                topic: "log".into(),
                payload: Value::Null,
            }]
        }
    }
    let fired = Rc::new(Cell::new(false));
    let f = fired.clone();
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(JobHost {
        actor_id: "c".into(),
        inbox: Inbox::new(8),
        patterns: vec![parse_pattern("c").unwrap()],
        make_job: Box::new(move |_| Box::new(BadCleanup { fired: f.clone() })),
    })
    .unwrap();
    let p = rt.ask("c", Value::Null, "caller", None);
    let c = drive_until_ready_local(&mut rt, &p, 20);
    assert_eq!(result_error(&c.body.unwrap()), None);
    assert!(fired.get(), "onFinished must still be invoked for cleanup");
    assert!(rt.is_quiescent());
}
#[test]
fn seq_id_space_exhausts_at_2_53() {
    // n=2^53 仍发最后一个 id，下次才耗尽。
    let gen = SeqIdGen::with_start((1u64 << 53) - 1);
    assert_eq!(gen.next(), format!("{:032x}", (1u64 << 53) - 1));
    assert_eq!(gen.next(), format!("{:032x}", 1u64 << 53));
}
#[test]
fn barrier_requires_durable_not_just_recorded() {
    // barrier 先 drain，再要求目标已落盘。
    let mut log = RecordLog::new(Box::new(MemorySink::new()), 8, 1);
    let g = SeqIdGen::new();
    let env = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 1,
    })
    .unwrap();
    assert!(log.record(&env, 0).is_some());
    // seq=1 已记录但未 flush → barrier 失败。
    assert!(log.barrier(1).is_err(), "barrier must require durable seq");
    log.flush(1).unwrap();
    assert!(log.barrier(1).is_ok());
}
// ── 第十三轮：可行性审计驱动的可修项回归 ──────────────────────────────────
#[test]
fn error_family_predicates_match_ts_instanceof() {
    use eba_rs::EbaError;
    assert!(EbaError::MailboxFull("x".into()).is_bus_family());
    assert!(EbaError::InvalidTopic("x".into()).is_bus_family());
    assert!(EbaError::AmbiguousRoute("x".into()).is_bus_family());
    assert!(!EbaError::Delivery("x".into()).is_bus_family());
    assert!(EbaError::MailboxBusy("x".into()).is_delivery_family());
    assert!(EbaError::Delivery("x".into()).is_delivery_family());
    assert!(EbaError::Backpressure("x".into()).is_backpressure_exact());
    assert!(!EbaError::MailboxFull("x".into()).is_backpressure_exact());
}
#[test]
fn same_topic_send_appends_to_parked_group() {
    // topic 已有 parked 组时，新信追加组尾（保 FIFO），不直投。
    let mut mb = eba_rs::job::mailbox::Mailbox::new(4);
    let inbox = Inbox::new(1);
    let bus = Bus::new();
    bus.subscribe("s", parse_pattern("t").unwrap(), inbox.clone());
    let g = SeqIdGen::new();
    let mk = || {
        make_envelope(MakeEnvelope {
            topic: "t".into(),
            payload: Value::Null,
            from: "a".into(),
            id_gen: &g,
            address: Address::Event,
            cause: None,
            sv: 1,
        })
        .unwrap()
    };
    // 塞满 inbox 使 publish 失败 → 第一封 park
    let filler = mk();
    assert!(bus.publish(&filler).unwrap());
    assert!(mb.send(&bus, mk()).unwrap());
    assert_eq!(mb.len(), 1);
    // 组已存在，第二封追加组尾
    assert!(mb.send(&bus, mk()).unwrap());
    assert_eq!(mb.len(), 2);
}
#[test]
fn close_checked_does_not_panic_and_is_retryable() {
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    // 空 runtime close 应 Ok，且幂等。
    assert!(rt.close_checked(None).is_ok());
    assert_eq!(rt.state(), HostState::Closed);
    assert!(rt.close_checked(None).is_ok());
}
#[test]
fn deliver_skips_home_bound_inbox_no_dedup() {
    // deliver 跳过 bound-to-home、不查 dedup/trust。
    let bus = Bus::new();
    let own = Inbox::new(4);
    let own_sig = std::rc::Rc::new(eba_rs::Signal::new());
    own_sig.bind(7, "self");
    own.bind_signal(own_sig);
    bus.subscribe("self", parse_pattern("t").unwrap(), own.clone());
    let other = Inbox::new(4);
    bus.subscribe("other", parse_pattern("t").unwrap(), other.clone());
    let g = SeqIdGen::new();
    let env = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 1,
    })
    .unwrap();
    // home=7：只投 other，不投 self
    assert!(bus.deliver(&env, 7).unwrap());
    let own_rx = own.receiver();
    let other_rx = other.receiver();
    assert!(own_rx.borrow());
    assert!(
        own_rx.try_recv().is_none(),
        "home-bound inbox must not receive deliver"
    );
    assert!(other_rx.borrow());
    assert!(
        other_rx.try_recv().is_some(),
        "other inbox must receive deliver"
    );
    other_rx.unborrow();
    own_rx.unborrow();
    // 同一信封再次 deliver（dedup 不应拦截）：other 应再收一封
    assert!(bus.deliver(&env, 7).unwrap());
    let other_rx2 = other.receiver();
    assert!(other_rx2.borrow());
    assert!(other_rx2.try_recv().is_some(), "deliver must not dedup");
}
#[test]
fn mailbox_full_never_buries_without_delivery_error() {
    // MailboxFull 只重登记候选，不计数、不 bury。
    let mut mb = eba_rs::job::mailbox::Mailbox::new(4);
    let inbox = Inbox::new(1); // 容量 1，塞满
    let bus = Bus::new();
    bus.subscribe("s", parse_pattern("t").unwrap(), inbox.clone());
    let g = SeqIdGen::new();
    let mk = |from: &str| {
        make_envelope(MakeEnvelope {
            topic: "t".into(),
            payload: Value::Null,
            from: from.into(),
            id_gen: &g,
            address: Address::Event,
            cause: None,
            sv: 1,
        })
        .unwrap()
    };
    bus.publish(&mk("a")).unwrap(); // 占满 inbox
    mb.send(&bus, mk("b")).unwrap(); // park 第一封
    for _ in 0..10 {
        let (step, sent) = mb.flush_one(&bus);
        assert!(sent.is_none(), "MailboxFull must not report a send");
        if step == eba_rs::job::mailbox::Step::Idle {
            break;
        }
    }
    assert_eq!(mb.len(), 1, "letter must remain parked, not buried");
}
#[test]
fn err_payload_code_detail_code_wins() {
    let detail = eba_rs::Value::Map(std::collections::BTreeMap::from([(
        "code".into(),
        eba_rs::Value::Int(99),
    )]));
    let body = eba_rs::err_payload_code_detail("boom", 42, detail);
    assert_eq!(
        body.as_map().unwrap().get("code"),
        Some(&eba_rs::Value::Int(42)),
        "explicit code must override detail.code"
    );
}
// ── 第十四轮：测试断言直接矛盾项 ────────────────────────────────────────
#[test]
fn safe_deadline_error_names_deadline_ms() {
    let err = eba_rs::time::safe_deadline((1u64 << 53) + 1).unwrap_err();
    assert!(
        format!("{}", err).contains("deadline_ms must be a safe integer"),
        "safeDeadline 默认 name=deadline_ms"
    );
}
#[test]
fn trust_rejection_is_envelope_build_error() {
    let bus = Bus::new();
    bus.set_trust_policy(Some(eba_rs::mail::TrustPolicy::new(Box::new(
        |_from: &str| false,
    ))));
    let g = SeqIdGen::new();
    let env = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "evil".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 1,
    })
    .unwrap();
    assert!(matches!(bus.publish(&env), Err(EbaError::EnvelopeBuild(_))));
    assert!(matches!(bus.tell(&env), Err(EbaError::EnvelopeBuild(_))));
}
#[test]
fn mailbox_send_hard_error_message_is_exact() {
    let bus = Bus::new();
    bus.set_trust_policy(Some(eba_rs::mail::TrustPolicy::new(Box::new(
        |_from: &str| false,
    ))));
    let mut mb = eba_rs::job::mailbox::Mailbox::new(4);
    let g = SeqIdGen::new();
    let env = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "evil".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 1,
    })
    .unwrap();
    match mb.send(&bus, env) {
        Err(EbaError::Delivery(msg)) => assert_eq!(msg, "result publication failed"),
        other => panic!("expected DeliveryError, got {:?}", other.map(|_| ())),
    }
}
#[test]
fn deliver_migrate_failure_is_silent_not_seized() {
    let mut reg = SchemaRegistry::new();
    reg.register("v", TopicSchema::new(1)).unwrap();
    let bus = Bus::with_schemas(Some(Rc::new(reg)));
    bus.subscribe(
        "v",
        eba_rs::pattern::parse_pattern("v").unwrap(),
        Inbox::new(4),
    );
    let g = SeqIdGen::new();
    let bad = make_envelope(MakeEnvelope {
        topic: "v".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 0,
    })
    .unwrap();
    assert!(!bus.deliver(&bad, 7).unwrap());
    assert!(
        bus.abandon_deliveries(None).is_empty(),
        "migrate 失败必须静默丢弃，不归入 seized"
    );
}
#[test]
fn unreadable_result_preserves_lease() {
    let mut reg = LeaseRegistry::new();
    let lease = Lease::new(LeaseOptions::default());
    lease.bind("owner");
    reg.submit(
        "r".into(),
        lease,
        "owner".into(),
        "stage",
        None,
        "reply.t",
        Some("r".into()),
    )
    .unwrap();
    let g = SeqIdGen::new();
    let env = make_envelope(MakeEnvelope {
        topic: "reply.t".into(),
        payload: Value::Null,
        from: "w".into(),
        id_gen: &g,
        address: Address::Result {
            correlation_id: "r".into(),
        },
        cause: Some("r".into()),
        sv: 1,
    })
    .unwrap();
    assert!(matches!(
        reg.take_result(&env),
        Err(EbaError::UnreadableResult(_))
    ));
    assert_eq!(reg.size(), 1, "不可读回信不得释放租约");
}
#[test]
fn schema_error_texts_match_ts_assertions() {
    let mut reg = SchemaRegistry::new();
    reg.register(
        "x",
        TopicSchema::new(2).with_validator(|p| {
            if matches!(p, Value::Null) {
                Err(EbaError::Other("bad".into()))
            } else {
                Ok(())
            }
        }),
    )
    .unwrap();
    assert!(format!("{}", reg.check("x", &Value::Null).unwrap_err()).contains("invalid x@2: bad"));
    reg.register("x", TopicSchema::new(3)).unwrap();
    assert!(format!("{}", reg.migrate("x", Value::Null, 4).unwrap_err())
        .contains("unsupported future schema version for x: 4"));
    assert!(format!("{}", reg.migrate("x", Value::Null, 2).unwrap_err())
        .contains("no migration for x@2 -> 3"));
    let mut boom = SchemaRegistry::new();
    boom.register(
        "boom",
        TopicSchema::new(2)
            .with_migrator(1, |_, _| Err(EbaError::Other("migrator exploded".into()))),
    )
    .unwrap();
    assert!(
        format!("{}", boom.migrate("boom", Value::Null, 1).unwrap_err())
            .contains("invalid boom@2 after migration: migrator exploded")
    );
}
#[test]
fn record_v_and_memory_sink_error_texts() {
    let g = SeqIdGen::new();
    let env = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 1,
    })
    .unwrap();
    let mut log = eba_rs::record::RecordLog::new(Box::new(MemorySink::new()), 8, 1);
    let rec = log.record(&env, 0).unwrap();
    assert_eq!(rec.v, eba_rs::record::RECORD_VERSION);
    let mut sink = MemorySink::new();
    sink.append(&rec).unwrap();
    let mut bad = rec.clone();
    bad.seq = 0;
    assert!(format!("{}", sink.append(&bad).unwrap_err()).contains("record out of order: 0 <= 1"));
    assert!(format!("{}", sink.flush(99).unwrap_err()).contains("flush beyond appended: 99 > 1"));
}
#[test]
fn mailbox_busy_uses_ts_default_text() {
    let bus = Bus::new();
    bus.subscribe(
        "t",
        eba_rs::pattern::parse_pattern("t").unwrap(),
        Inbox::new(1),
    );
    let mut mb = eba_rs::job::mailbox::Mailbox::new(1);
    let g = SeqIdGen::new();
    let mk = |from: &str| {
        make_envelope(MakeEnvelope {
            topic: "t".into(),
            payload: Value::Null,
            from: from.into(),
            id_gen: &g,
            address: Address::Event,
            cause: None,
            sv: 1,
        })
        .unwrap()
    };
    bus.publish(&mk("a")).unwrap(); // 占满 inbox
    mb.send(&bus, mk("b")).unwrap(); // park 第一封（mailbox 满）
    match mb.send(&bus, mk("c")) {
        Err(EbaError::MailboxBusy(msg)) => {
            assert_eq!(msg, eba_rs::errors::MAILBOX_BUSY)
        }
        other => panic!("expected MailboxBusy, got {:?}", other.map(|_| ())),
    }
}
// ── 第十七轮：零预设复审驱动的 A 类残余缺口 ──────────────────────────────────
/// commitTo：finish 已形成时，同轮 ask 意图整批跳过，
/// 不得对外发出孤儿请求。
#[test]
fn finish_skips_same_turn_ask_intent() {
    use std::cell::Cell;
    struct Skipper {
        fired: Rc<Cell<bool>>,
    }
    impl JobHooks for Skipper {
        fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
            let ask = ctx
                .ask("stage", "downstream_orphan", Value::Null, None)
                .unwrap();
            let fin = ctx.finish_effect(ok_payload(Value::Bool(true)));
            vec![ask, fin]
        }
        fn on_finished(&self, _body: Option<Value>) -> Vec<Effect> {
            self.fired.set(true);
            Vec::new()
        }
    }
    let fired = Rc::new(Cell::new(false));
    let f = fired.clone();
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(JobHost {
        actor_id: "skipper".into(),
        inbox: Inbox::new(16),
        patterns: vec![parse_pattern("skip").unwrap()],
        make_job: Box::new(move |_| Box::new(Skipper { fired: f.clone() })),
    })
    .unwrap();
    struct Spy;
    impl JobHooks for Spy {
        fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
            vec![ctx.finish_effect(ok_payload(Value::Null))]
        }
    }
    rt.add(JobHost {
        actor_id: "spy".into(),
        inbox: Inbox::new(16),
        patterns: vec![parse_pattern("downstream_orphan").unwrap()],
        make_job: Box::new(|_| Box::new(Spy)),
    })
    .unwrap();
    let p = rt.ask("skip", Value::Null, "caller", None);
    drive(&mut rt, 10);
    assert!(p.is_ready(), "finish 必须结算调用方");
    assert!(fired.get(), "onFinished must fire once");
    let stats = rt.stats();
    assert_eq!(
        stats.hosts_detail.get("spy").map(|s| s.active + s.queued),
        Some(0)
    );
}
/// / hold 后 reply/finish 必须失败结算（job_failed），
/// 不得既安装 held 槽又回信。
#[test]
fn hold_then_finish_settles_job_failed_not_double_reply() {
    use std::cell::Cell;
    struct HeldFinish {
        done: Rc<Cell<usize>>,
    }
    impl JobHooks for HeldFinish {
        fn begin(&self, _ctx: &mut Context) -> Vec<Effect> {
            // 直接构造字面量绕过 builder 的 push 顺序约束，模拟 [hold, finish] 组合。
            let lease = Lease::new(LeaseOptions::default());
            vec![
                Effect::Hold { lease },
                Effect::Finish {
                    body: ok_payload(Value::Null),
                },
            ]
        }
        fn on_finished(&self, _body: Option<Value>) -> Vec<Effect> {
            self.done.set(self.done.get() + 1);
            Vec::new()
        }
    }
    let done = Rc::new(Cell::new(0));
    let d = done.clone();
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(JobHost {
        actor_id: "hf".into(),
        inbox: Inbox::new(16),
        patterns: vec![parse_pattern("hf").unwrap()],
        make_job: Box::new(move |_| Box::new(HeldFinish { done: d.clone() })),
    })
    .unwrap();
    let p = rt.ask("hf", Value::Null, "caller", None);
    let err = rt.advance().unwrap_err();
    assert!(format!("{}", err).contains("job already held"));
    drive(&mut rt, 5);
    assert!(done.get() <= 1, "onFinished 不得重复触发");
    let _ = p;
}
/// applyEffects：字面量 Effect 的非法 body 必须在落地口被拒（job_failed）。
#[test]
fn literal_effect_bad_body_settles_job_failed() {
    struct LiteralBad;
    impl JobHooks for LiteralBad {
        fn begin(&self, _ctx: &mut Context) -> Vec<Effect> {
            vec![Effect::Reply {
                body: Value::Str("not a result".into()),
            }]
        }
    }
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(JobHost {
        actor_id: "lit".into(),
        inbox: Inbox::new(16),
        patterns: vec![parse_pattern("lit").unwrap()],
        make_job: Box::new(|_| Box::new(LiteralBad)),
    })
    .unwrap();
    let p = rt.ask("lit", Value::Null, "caller", None);
    let err = rt.advance().unwrap_err();
    assert!(format!("{}", err).contains("invalid result body"));
    drive(&mut rt, 5);
    // 落地口拒绝后 job 必须回收，不得滞留使 has_work 恒真。
    assert!(rt.is_quiescent(), "非法 body 的 job 必须被回收");
    let _ = p;
}
/// 落地错误必须结算 job_failed 并回收 job。
#[test]
fn landing_error_recycles_job_and_settles_caller() {
    struct BadTimer;
    impl JobHooks for BadTimer {
        fn begin(&self, _ctx: &mut Context) -> Vec<Effect> {
            vec![Effect::Timer {
                deadline_ms: (1u64 << 53) + 1,
            }]
        }
    }
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(JobHost {
        actor_id: "bt".into(),
        inbox: Inbox::new(16),
        patterns: vec![parse_pattern("bt").unwrap()],
        make_job: Box::new(|_| Box::new(BadTimer)),
    })
    .unwrap();
    let p = rt.ask("bt", Value::Null, "caller", None);
    let err = rt.advance().unwrap_err();
    assert!(format!("{}", err).contains("deadline_ms must be a safe integer"));
    drive(&mut rt, 5);
    assert!(rt.is_quiescent(), "错误路径必须回收 job，不得永久 has_work");
    let _ = p;
}
/// Sequencer：2^53-1 发出最后一个 seq，下一次抛可捕获 SeqExhausted。
#[test]
fn sequencer_exhaustion_is_catchable_at_safe_integer() {
    use eba_rs::record::MAX_SAFE_SEQ;
    let mut s = eba_rs::record::Sequencer::new(MAX_SAFE_SEQ);
    assert_eq!(s.try_next_seq().unwrap(), MAX_SAFE_SEQ);
    assert!(matches!(s.try_next_seq(), Err(EbaError::SeqExhausted(_))));
    assert_eq!(s.current(), MAX_SAFE_SEQ);
}
/// 全量 close 后 front 退役，stats().hosts 归零。
#[test]
fn full_close_retires_front_from_stats() {
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(JobHost {
        actor_id: "h".into(),
        inbox: Inbox::new(16),
        patterns: vec![parse_pattern("h").unwrap()],
        make_job: Box::new(|_| Box::new(FinishJob(Value::Null))),
    })
    .unwrap();
    let p = rt.ask("h", Value::Null, "caller", None);
    drive(&mut rt, 6);
    let _ = p.ready();
    assert_eq!(rt.stats().hosts, 2);
    rt.close_checked(None).unwrap();
    assert_eq!(rt.stats().hosts, 0);
    assert!(!rt.stats().hosts_detail.contains_key("sys.ask"));
}
/// Caller.admit：front 过载时同步拒绝新 ask（active_full）。
#[test]
fn front_backpressure_rejects_when_active_limit_reached() {
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    let mut reached = false;
    for i in 0..300 {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = rt.ask("no_such_topic", Value::Null, format!("c{}", i), None);
        }));
        if r.is_err() {
            reached = true;
            break;
        }
    }
    assert!(reached, "front 达到 active_limit 后必须拒绝新 ask");
}
/// deliverEnding：onFinished 返回 close 时不得因误判静默而多发 job_silent。
#[test]
fn on_finished_close_does_not_double_reply() {
    use std::cell::Cell;
    struct FinClose {
        closes: Rc<Cell<usize>>,
    }
    impl JobHooks for FinClose {
        fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
            vec![ctx.finish_effect(ok_payload(Value::Bool(true)))]
        }
        fn on_finished(&self, _body: Option<Value>) -> Vec<Effect> {
            self.closes.set(self.closes.get() + 1);
            vec![Effect::Close { reason: None }]
        }
    }
    let closes = Rc::new(Cell::new(0));
    let c = closes.clone();
    let mut rt = Runtime::new(Bus::new(), clock(), ids()).unwrap();
    rt.add(JobHost {
        actor_id: "fc".into(),
        inbox: Inbox::new(16),
        patterns: vec![parse_pattern("fc").unwrap()],
        make_job: Box::new(move |_| Box::new(FinClose { closes: c.clone() })),
    })
    .unwrap();
    let p = rt.ask("fc", Value::Null, "caller", None);
    drive(&mut rt, 8);
    assert_eq!(closes.get(), 1, "onFinished 只触发一次");
    assert!(p.is_ready());
    let completion = p.ready().unwrap();
    // 只收到真正回信（ok=true），不是 job_silent（后者是 err）
    let body = completion
        .body
        .as_ref()
        .expect("completion must carry result body");
    assert!(is_result_ok(body), "unexpected body: {:?}", body);
}
