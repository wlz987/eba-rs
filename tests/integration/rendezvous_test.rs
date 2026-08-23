use std::rc::Rc;

use crate::util::{env_gen, object, read_job, result_echo, seq};
use eba::{Bus, Inbox, Value};

#[test]
fn cover_poll_empty_still_watchdog() {
    let bus = Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let mut host = eba::JobHost::new(eba::HostParams {
        actor_id: crate::util::actor_h(),
        inbox: Inbox::new(4),
        patterns: vec!["read".into()],
        accept: None,
        request_timeout_ms: 0,
        queue_limit: 0,
        make_job: Box::new(|root| read_job(root.clone())),
    });
    let ok = host.poll(bus.clone(), clk.clone(), gen.clone()).unwrap();
    assert!(!ok, "empty poll must still sweep the watchdog");
    let env = env_gen("read", Value::Int(1), &gen);
    bus.subscribe("read", host.inbox()).unwrap();
    bus.publish(&env).unwrap();
    let ok = host.poll(bus, clk, gen).unwrap();
    assert!(ok);
}

#[test]
fn shutdown_skips_business_routes_result() {
    let bus = Bus::new();
    let clk = Rc::new(eba::ManualClock::new(0));
    let gen = seq(1);
    let (mut host, states) = crate::util::step_host(Inbox::new(8), 0, 0);
    host.start(&bus).unwrap();
    let root = env_gen("job", Value::Null, &gen);
    host.handle(bus.clone(), &root, clk.clone(), gen.clone())
        .unwrap();
    let child_id = states.borrow()[0].child_id.clone().expect("child id");
    let mut host = host;
    host.shutdown();
    let extra = env_gen("job", object(vec![("later", Value::Int(1))]), &gen);
    host.handle(bus.clone(), &extra, clk.clone(), gen.clone())
        .unwrap();
    assert!(
        host.job(&extra.header.cause).is_none(),
        "shutdown must not adopt new jobs"
    );
    let echo = result_echo(
        &child_id,
        &root.header.cause,
        eba::ok_payload(Value::Str("allow".into())),
        &gen,
    );
    host.handle(bus, &echo, clk, gen).unwrap();
    assert!(
        !states.borrow()[0].seen.is_empty(),
        "in-flight result must still resolve after shutdown"
    );
}
