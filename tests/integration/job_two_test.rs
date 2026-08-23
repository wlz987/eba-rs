use std::rc::Rc;

use crate::util::{echo_job, env_gen, leaf_host, object, read_job, seq, step_host};
use eba::{is_result_ok, result_value, Bus, Inbox, Value};

#[test]
fn sync_begin_reply_finish() {
    let bus = Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let mut host = eba::JobHost::new(eba::HostParams {
        actor_id: crate::util::actor_h(),
        inbox: Inbox::new(8),
        patterns: vec!["read".into()],
        accept: None,
        request_timeout_ms: 0,
        queue_limit: 0,
        make_job: Box::new(|root| read_job(root.clone())),
    });
    host.start(&bus).unwrap();
    let dest = Inbox::new(8);
    bus.subscribe("read.result.**", &dest).unwrap();
    host.handle(
        bus.clone(),
        &env_gen("read", Value::Str("buf.a".into()), &gen),
        clk.clone(),
        gen.clone(),
    )
    .unwrap();
    let got = dest.try_recv().expect("reply must be delivered");
    let result = got.payload.get("result").cloned().expect("result key");
    assert_eq!(result_value(&result), Some(&Value::Str("buf.a".into())));
    assert!(host.job(&got.header.cause).is_none());
}

#[test]
fn request_then_stage() {
    let bus = Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let (mut parent, _states) = step_host(Inbox::new(8), 0, 0);
    let mut leaf = leaf_host();
    parent.start(&bus).unwrap();
    leaf.start(&bus).unwrap();
    let dest = Inbox::new(8);
    bus.subscribe("job.result.**", &dest).unwrap();

    let root = env_gen("job", Value::Null, &gen);
    parent
        .handle(bus.clone(), &root, clk.clone(), gen.clone())
        .unwrap();
    let child = leaf
        .inbox()
        .try_recv()
        .expect("leaf must receive the request");
    assert_eq!(child.header.cause, root.header.cause);
    leaf.handle(bus.clone(), &child, clk.clone(), gen.clone())
        .unwrap();
    let result = parent.inbox().try_recv().expect("parent gets stage result");
    parent
        .handle(bus.clone(), &result, clk.clone(), gen.clone())
        .unwrap();
    let done = dest.try_recv().expect("root reply delivered");
    assert!(is_result_ok(done.payload.get("result").unwrap()));
}

#[test]
fn same_inbox_request_drains() {
    let bus = Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let states = crate::util::new_states();
    let mut host = crate::util::combo_host(states);
    host.start(&bus).unwrap();
    let dest = Inbox::new(8);
    bus.subscribe("job.result.**", &dest).unwrap();
    host.handle(
        bus.clone(),
        &env_gen("job", Value::Null, &gen),
        clk.clone(),
        gen.clone(),
    )
    .unwrap();
    let done = dest
        .try_recv()
        .expect("same-inbox drain finishes in one handle");
    assert!(is_result_ok(done.payload.get("result").unwrap()));
}

#[test]
fn clock_watchdog() {
    let bus = Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let (mut parent, states) = step_host(Inbox::new(8), 10, 0);
    parent.start(&bus).unwrap();
    let root = env_gen("job", Value::Null, &gen);
    parent
        .handle(bus.clone(), &root, clk.clone(), gen.clone())
        .unwrap();
    assert!(
        parent.job(&root.header.cause).is_some(),
        "job must be on stage"
    );
    clk.advance(10);
    let ok = parent.poll(bus, clk, gen).unwrap();
    assert!(!ok, "watchdog poll consumes nothing");
    let st = states.borrow();
    assert_eq!(st[0].seen, vec!["acl".to_string()]);
    drop(st);
    assert!(
        parent.job(&root.header.cause).map(|j| j.is_finished()) != Some(false),
        "timed-out job must be finished"
    );
}

#[test]
fn deferred_reply_roundtrip() {
    let bus = Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let (mut host, begins) = crate::util::wait_host(0);
    host.start(&bus).unwrap();
    let dest = Inbox::new(8);
    let held = Inbox::new(8);
    bus.subscribe("wait.result.**", &dest).unwrap();
    bus.subscribe("ext.wait.**", &held).unwrap();
    let root = env_gen("wait", Value::Null, &gen);
    host.handle(bus.clone(), &root, clk.clone(), gen.clone())
        .unwrap();
    assert!(host.job(&root.header.cause).is_some());
    assert_eq!(begins.get(), 1);
    let req = held.try_recv().expect("held request");
    eba::Matchmaker::new(req)
        .reply(
            &bus,
            &eba::ok_payload(Value::Str("ready".into())),
            "ext.wait.result",
            crate::util::actor_l(),
            &gen,
        )
        .unwrap();
    let result = host.inbox().try_recv().expect("result");
    host.handle(bus, &result, clk, gen).unwrap();
    assert!(dest.try_recv().is_some(), "root reply");
    assert_eq!(begins.get(), 1);
    assert!(host.job(&root.header.cause).is_none());
}

#[test]
fn deferred_reply_conjunction() {
    let bus = Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let left = Rc::new(std::cell::Cell::new(2u8));
    let left_b = left.clone();
    let mut host = eba::JobHost::new(eba::HostParams {
        actor_id: crate::util::actor_h(),
        inbox: Inbox::new(8),
        patterns: vec!["job".into()],
        accept: None,
        request_timeout_ms: 0,
        queue_limit: 0,
        make_job: Box::new(move |root| {
            let left = left_b.clone();
            let mut j = eba::Job::new(root.clone());
            j.on_begin = Some(Box::new(|ctx| {
                ctx.request(eba::RequestSpec {
                    stage: "a".into(),
                    request_prefix: "ext.a".into(),
                    result_prefix: "ext.a.result".into(),
                    payload: Value::Int(1),
                })?;
                ctx.request(eba::RequestSpec {
                    stage: "b".into(),
                    request_prefix: "ext.b".into(),
                    result_prefix: "ext.b.result".into(),
                    payload: Value::Int(2),
                })?;
                Ok(())
            }));
            j.on_stage_result = Some(Box::new(move |ctx, _stage, _body| {
                left.set(left.get() - 1);
                if left.get() == 0 {
                    ctx.finish(eba::FinishInfo::ok_answer(Value::Null))?;
                }
                Ok(())
            }));
            j
        }),
    });
    host.start(&bus).unwrap();
    let a_box = Inbox::new(8);
    let b_box = Inbox::new(8);
    bus.subscribe("ext.a.**", &a_box).unwrap();
    bus.subscribe("ext.b.**", &b_box).unwrap();
    let root = env_gen("job", Value::Null, &gen);
    host.handle(bus.clone(), &root, clk.clone(), gen.clone())
        .unwrap();
    let ra = a_box.try_recv().expect("a");
    let rb = b_box.try_recv().expect("b");
    eba::Matchmaker::new(ra)
        .reply(
            &bus,
            &eba::ok_payload(Value::Int(1)),
            "ext.a.result",
            crate::util::actor_l(),
            &gen,
        )
        .unwrap();
    eba::Matchmaker::new(rb)
        .reply(
            &bus,
            &eba::ok_payload(Value::Int(2)),
            "ext.b.result",
            crate::util::actor_l(),
            &gen,
        )
        .unwrap();
    for _ in 0..2 {
        let result = host.inbox().try_recv().expect("result");
        host.handle(bus.clone(), &result, clk.clone(), gen.clone())
            .unwrap();
    }
    assert!(host.job(&root.header.cause).is_none());
}

#[test]
fn root_cause_unique() {
    let bus = Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let (mut host, _begins) = crate::util::wait_host(0);
    let root = env_gen("wait", Value::Null, &gen);
    host.handle(bus.clone(), &root, clk.clone(), gen.clone())
        .unwrap();
    let dup = eba::make_envelope(
        "wait",
        Value::Null,
        crate::util::actor_a(),
        eba::MakeOptions {
            id: Some(root.header.id.clone()),
            cause: Some(root.header.cause.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    let err = host.handle(bus, &dup, clk, gen).unwrap_err();
    assert!(
        matches!(err, eba::Error::State(ref m) if m.contains("duplicate")),
        "duplicate cause must be a violation: {err}"
    );
}

#[test]
fn leaf_ids_share_cause() {
    let bus = Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let mut leaf = eba::JobHost::new(eba::HostParams {
        actor_id: crate::util::actor_l(),
        inbox: Inbox::new(8),
        patterns: vec!["acl.*".into()],
        accept: None,
        request_timeout_ms: 0,
        queue_limit: 0,
        make_job: Box::new(|root| echo_job(root.clone())),
    });
    let cause = gen.borrow_mut().next_envelope_id();
    let a = gen.borrow_mut().next_envelope_id();
    let b = gen.borrow_mut().next_envelope_id();
    let seg_a = eba::topic_segment(&a).unwrap();
    let seg_b = eba::topic_segment(&b).unwrap();
    let env_a = eba::make_envelope(
        &format!("acl.{seg_a}"),
        Value::Int(1),
        crate::util::actor_a(),
        eba::MakeOptions {
            id: Some(a.clone()),
            cause: Some(cause.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    let env_b = eba::make_envelope(
        &format!("acl.{seg_b}"),
        Value::Int(2),
        crate::util::actor_a(),
        eba::MakeOptions {
            id: Some(b.clone()),
            cause: Some(cause.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    leaf.handle(bus.clone(), &env_a, clk.clone(), gen.clone())
        .unwrap();
    leaf.handle(bus, &env_b, clk, gen).unwrap();
    assert!(leaf.job(&a).is_none());
    assert!(leaf.job(&b).is_none());
}

#[test]
fn unmatched_and_two_active() {
    let bus = Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let (mut host, _states) = step_host(Inbox::new(8), 0, 1);
    host.start(&bus).unwrap();
    host.handle(
        bus.clone(),
        &env_gen("other", Value::Int(1), &gen),
        clk.clone(),
        gen.clone(),
    )
    .unwrap();
    let first = env_gen("job", Value::Null, &gen);
    let second = env_gen("job", object(vec![("pad", Value::Int(1))]), &gen);
    host.handle(bus.clone(), &first, clk.clone(), gen.clone())
        .unwrap();
    host.handle(bus, &second, clk, gen).unwrap();
    assert!(
        host.job(&first.header.cause).is_some(),
        "two active jobs must coexist on stage"
    );
    assert!(host.job(&second.header.cause).is_some());
}
