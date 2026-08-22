use std::cell::RefCell;
use std::rc::Rc;

use crate::util::{
    env_gen, err_body, leaf_host, new_states, result_echo, seq, step_host,
    step_job,
};
use eba::{is_result_err, FinishInfo, Job, JobHost, RequestSpec, Value};

fn narrow_accept_host(
) -> (JobHost, eba::BusHandle, eba::IdGenHandle, Rc<eba::ManualClock>) {
    let bus = eba::Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let states = new_states();
    let st = states.clone();
    let host = JobHost::new(eba::HostParams {
        actor_id: crate::util::actor_h(),
        inbox: eba::Inbox::new(8),
        patterns: vec!["job.**".into()],
        accept: Some(vec!["job".into()]),
        request_timeout_ms: 0,
        queue_limit: 0,
        make_job: Box::new(move |root| step_job(root.clone(), &st)),
    });
    (host, bus, gen, clk)
}

#[test]
fn accept_narrower_than_subscribe() {
    let (mut host, bus, gen, clk) = narrow_accept_host();
    host.start(&bus).unwrap();
    let extra = env_gen("job.extra", Value::Null, &gen);
    host.handle(bus.clone(), &extra, clk.clone(), gen.clone())
        .unwrap();
    assert!(host.job(&extra.header.cause).is_none());
    let root = env_gen("job", Value::Null, &gen);
    host.handle(bus, &root, clk, gen).unwrap();
    assert!(host.job(&root.header.cause).is_some());
}

#[test]
fn complete_parked_missing() {
    let (mut host, bus, gen, clk) = missing_free_parked_host();
    let missing = gen.borrow_mut().next_envelope_id();
    let err = host
        .complete_parked(
            bus,
            &missing,
            clk,
            gen,
            "wait.result",
            &eba::ok_payload(Value::Int(1)),
        )
        .unwrap_err();
    assert!(matches!(err, eba::Error::State(ref m) if m.contains("parked job not found")));
}

fn missing_free_parked_host() -> (
    JobHost,
    eba::BusHandle,
    eba::IdGenHandle,
    Rc<eba::ManualClock>,
) {
    let (host, _begins) = crate::util::parked_host(0);
    let bus = eba::Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    (host, bus, gen, clk)
}

#[test]
fn complete_parked_err() {
    let bus = eba::Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let (mut host, _begins) = crate::util::parked_host(0);
    let dest = eba::Inbox::new(8);
    bus.subscribe("wait.result.**", &dest).unwrap();
    let root = env_gen("wait", Value::Null, &gen);
    host.handle(bus.clone(), &root, clk.clone(), gen.clone())
        .unwrap();
    assert!(host.parked(&root.header.cause).is_some());
    assert!(host.parked(&root.header.cause).unwrap().is_parked());
    host.complete_parked(
        bus.clone(),
        &root.header.cause,
        clk.clone(),
        gen.clone(),
        "wait.result",
        &err_body("later"),
    )
    .unwrap();
    let got = dest.try_recv().expect("err completion must still reply");
    assert!(is_result_err(got.payload.get("result").unwrap()));
    drop(host);
}

#[test]
fn park_after_finish() {
    let bus = eba::Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let mut host = JobHost::new(eba::HostParams {
        actor_id: crate::util::actor_h(),
        inbox: eba::Inbox::new(8),
        patterns: vec!["job".into()],
        accept: None,
        request_timeout_ms: 0,
        queue_limit: 0,
        make_job: Box::new(|root| {
            let mut j = Job::new(root.clone());
            j.on_begin = Some(Box::new(|ctx| {
                ctx.finish(FinishInfo::ok_answer(Value::Null))?;
                ctx.park()
            }));
            j
        }),
    });
    let err = host
        .handle(bus, &env_gen("job", Value::Null, &gen), clk, gen)
        .unwrap_err();
    assert!(matches!(err, eba::Error::State(ref m) if m.contains("not requestable")));
}

#[test]
fn finish_idempotent() {
    let ends: Rc<RefCell<Vec<bool>>> = Rc::new(RefCell::new(vec![]));
    let bus = eba::Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let ends2 = ends.clone();
    let mut host = JobHost::new(eba::HostParams {
        actor_id: crate::util::actor_h(),
        inbox: eba::Inbox::new(8),
        patterns: vec!["job".into()],
        accept: None,
        request_timeout_ms: 0,
        queue_limit: 0,
        make_job: Box::new(move |root| {
            let ends2 = ends2.clone();
            let mut j = Job::new(root.clone());
            j.on_begin = Some(Box::new(|ctx| {
                ctx.finish(FinishInfo::ok_answer(Value::Int(1)))?;
                ctx.finish(FinishInfo::err_msg("no"))
            }));
            j.on_finished = Some(Box::new(move |_ctx, info| {
                ends2.borrow_mut().push(info.ok);
                Ok(())
            }));
            j
        }),
    });
    let root = env_gen("job", Value::Null, &gen);
    host.handle(bus, &root, clk, gen).unwrap();
    assert_eq!(*ends.borrow(), vec![true]);
    assert!(host.job(&root.header.cause).is_none());
}

#[test]
fn begin_error_drops_slot() {
    let bus = eba::Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let mut host = JobHost::new(eba::HostParams {
        actor_id: crate::util::actor_h(),
        inbox: eba::Inbox::new(8),
        patterns: vec!["job".into()],
        accept: None,
        request_timeout_ms: 0,
        queue_limit: 0,
        make_job: Box::new(|root| {
            let mut j = Job::new(root.clone());
            j.on_begin = Some(Box::new(|_ctx| Err(eba::Error::State("boom".into()))));
            j
        }),
    });
    let root = env_gen("job", Value::Null, &gen);
    let err = host.handle(bus, &root, clk, gen).unwrap_err();
    assert!(matches!(err, eba::Error::State(ref m) if m == "boom"));
    assert!(host.job(&root.header.cause).is_none());
}

#[test]
fn no_timeout_does_not_expire() {
    let bus = eba::Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let (mut host, _states) = step_host(eba::Inbox::new(8), 0, 0);
    host.start(&bus).unwrap();
    let root = env_gen("job", Value::Null, &gen);
    host.handle(bus.clone(), &root, clk.clone(), gen.clone())
        .unwrap();
    assert!(host.job(&root.header.cause).is_some());
    clk.advance(10_000);
    host.poll(bus, clk, gen).unwrap();
    assert!(
        host.job(&root.header.cause).is_some(),
        "without timeout the watchdog must leave the job alone"
    );
}

#[test]
fn parked_not_swept_by_clock() {
    let bus = eba::Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let (mut host, _begins) = crate::util::parked_host(1);
    let root = env_gen("wait", Value::Null, &gen);
    host.handle(bus.clone(), &root, clk.clone(), gen.clone())
        .unwrap();
    clk.advance(100);
    host.poll(bus, clk, gen).unwrap();
    assert!(
        host.parked(&root.header.cause).is_some(),
        "watchdog must not sweep parked jobs"
    );
}

#[test]
fn late_result_after_timeout() {
    let bus = eba::Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let (mut host, states) = step_host(eba::Inbox::new(8), 5, 0);
    host.start(&bus).unwrap();
    let root = env_gen("job", Value::Null, &gen);
    host.handle(bus.clone(), &root, clk.clone(), gen.clone())
        .unwrap();
    let child_id = states.borrow()[0].child_id.clone().expect("child id");
    clk.advance(5);
    host.poll(bus.clone(), clk.clone(), gen.clone()).unwrap();
    assert_eq!(states.borrow()[0].seen, vec!["acl".to_string()]);
    let echo = result_echo(
        &child_id,
        &root.header.cause,
        eba::ok_payload(Value::Str("late".into())),
        &gen,
    );
    host.handle(bus, &echo, clk, gen).unwrap();
    assert!(
        host.job(&root.header.cause).map(|j| j.is_finished()) != Some(false),
        "late result must find no live job"
    );
}

#[test]
fn max_inflight() {
    let bus = eba::Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let mut host = JobHost::new(eba::HostParams {
        actor_id: crate::util::actor_h(),
        inbox: eba::Inbox::new(8),
        patterns: vec!["job".into()],
        accept: None,
        request_timeout_ms: 0,
        queue_limit: 0,
        make_job: Box::new(|root| {
            let mut j = Job::new(root.clone());
            j.set_max_inflight(1);
            j.on_begin = Some(Box::new(|ctx| {
                let spec = |payload| RequestSpec {
                    stage: "a".into(),
                    request_prefix: "acl".into(),
                    result_prefix: "acl.result".into(),
                    payload,
                };
                ctx.request(spec(Value::Int(1)))?;
                ctx.request(spec(Value::Int(2)))?;
                Ok(())
            }));
            j
        }),
    });
    host.start(&bus).unwrap();
    let err = host
        .handle(bus, &env_gen("job", Value::Null, &gen), clk, gen)
        .unwrap_err();
    assert!(matches!(err, eba::Error::MaxInflight));
}

#[test]
fn continuation_defers_second_request() {
    let bus = eba::Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let stages: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(vec![]));
    let stages2 = stages.clone();
    let mut parent = JobHost::new(eba::HostParams {
        actor_id: crate::util::actor_h(),
        inbox: eba::Inbox::new(8),
        patterns: vec!["job".into()],
        accept: None,
        request_timeout_ms: 0,
        queue_limit: 0,
        make_job: Box::new(move |root| {
            let mut j = Job::new(root.clone());
            let sc = stages2.clone();
            j.on_begin = Some(Box::new(|ctx| {
                ctx.request(RequestSpec {
                    stage: "one".into(),
                    request_prefix: "acl".into(),
                    result_prefix: "acl.result".into(),
                    payload: Value::Int(1),
                })?;
                Ok(())
            }));
            j.on_stage_result = Some(Box::new(move |ctx, stage, _body| {
                sc.borrow_mut().push(stage.to_string());
                if stage == "one" {
                    ctx.request(RequestSpec {
                        stage: "two".into(),
                        request_prefix: "acl".into(),
                        result_prefix: "acl.result".into(),
                        payload: Value::Int(2),
                    })?;
                    return Ok(());
                }
                ctx.finish(FinishInfo::ok_answer(Value::Str(stage.into())))
            }));
            j
        }),
    });
    let mut leaf = leaf_host();
    parent.start(&bus).unwrap();
    leaf.start(&bus).unwrap();
    let root = env_gen("job", Value::Null, &gen);
    parent
        .handle(bus.clone(), &root, clk.clone(), gen.clone())
        .unwrap();
    for round in 0..2 {
        let req = leaf
            .inbox()
            .try_recv()
            .unwrap_or_else(|| panic!("round {round}: leaf must receive the request"));
        leaf.handle(bus.clone(), &req, clk.clone(), gen.clone())
            .unwrap();
        let r = parent
            .inbox()
            .try_recv()
            .unwrap_or_else(|| panic!("round {round}: parent gets stage result"));
        parent
            .handle(bus.clone(), &r, clk.clone(), gen.clone())
            .unwrap();
    }
    assert_eq!(*stages.borrow(), vec!["one".to_string(), "two".to_string()]);
    assert!(
        parent
            .job(&root.header.cause)
            .map(|j| j.is_finished())
            != Some(false),
        "chained job must be finished"
    );
    assert!(parent.parked(&root.header.cause).is_none());
}

#[test]
fn sync_reply_cause_and_new_id() {
    let bus = eba::Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let mut host = JobHost::new(eba::HostParams {
        actor_id: crate::util::actor_h(),
        inbox: eba::Inbox::new(8),
        patterns: vec!["read".into()],
        accept: None,
        request_timeout_ms: 0,
        queue_limit: 0,
        make_job: Box::new(|root| crate::util::read_job(root.clone())),
    });
    host.start(&bus).unwrap();
    let dest = eba::Inbox::new(8);
    bus.subscribe("read.result.**", &dest).unwrap();
    let root = env_gen("read", Value::Str("z".into()), &gen);
    host.handle(bus, &root, clk, gen).unwrap();
    let got = dest.try_recv().expect("reply arrives");
    assert_eq!(got.header.cause, root.header.cause);
    assert_ne!(got.header.id, root.header.id);
    assert_eq!(
        got.payload.get("request_id"),
        Some(&Value::Str(root.header.id.0.clone()))
    );
}

#[test]
fn request_after_park_rejected() {
    let bus = eba::Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let mut host = JobHost::new(eba::HostParams {
        actor_id: crate::util::actor_h(),
        inbox: eba::Inbox::new(8),
        patterns: vec!["job".into()],
        accept: None,
        request_timeout_ms: 0,
        queue_limit: 0,
        make_job: Box::new(|root| {
            let mut j = Job::new(root.clone());
            j.on_begin = Some(Box::new(|ctx| {
                ctx.park()?;
                ctx.request(RequestSpec {
                    stage: "a".into(),
                    request_prefix: "acl".into(),
                    result_prefix: "acl.result".into(),
                    payload: Value::Int(1),
                })?;
                Ok(())
            }));
            j
        }),
    });
    host.start(&bus).unwrap();
    let err = host
        .handle(bus, &env_gen("job", Value::Null, &gen), clk, gen)
        .unwrap_err();
    assert!(matches!(err, eba::Error::State(ref m) if m.contains("not requestable")));
}

#[test]
fn result_err_finishes_step() {
    let bus = eba::Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let (mut host, states) = step_host(eba::Inbox::new(8), 0, 0);
    host.start(&bus).unwrap();
    let root = env_gen("job", Value::Null, &gen);
    host.handle(bus.clone(), &root, clk.clone(), gen.clone())
        .unwrap();
    let child_id = states.borrow()[0].child_id.clone().expect("child id");
    let echo = result_echo(&child_id, &root.header.cause, err_body("deny"), &gen);
    host.handle(bus, &echo, clk, gen).unwrap();
    assert_eq!(states.borrow()[0].seen, vec!["acl".to_string()]);
    assert!(
        !host.job(&root.header.cause).map(|j| !j.is_finished()).unwrap_or(false),
        "err result must finish the step"
    );
}
