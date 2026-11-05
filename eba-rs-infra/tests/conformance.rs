//! EBA conformance harness (rs).
//!
//! Mirrors `eba-ts/test/conformance.mjs` and the scenarios in
//! `conformance/base.txt`: effect.resume, lease.revoke, record.ring,
//! record.log and effect.host. Uses `ManualClock` + manual `advance()` for
//! determinism.

use core::cell::RefCell;
use std::rc::Rc;
use std::string::String;
use std::sync::Arc;
use std::vec::Vec;

use eba_rs_infra::envelope::idgen::{IdGen, SeqIdGen};
use eba_rs_infra::envelope::ids::actor_id;
use eba_rs_infra::envelope::result::ok_payload;
use eba_rs_infra::envelope::{make_envelope, Envelope, MakeEnvelope};
use eba_rs_infra::errors::BoxError;
use eba_rs_infra::job::{
    Context, Core, CoreOptions, Effect, Job, JobHooks, JobHost, JobLease, Mailbox,
};
use eba_rs_infra::mail::bus::Bus;
use eba_rs_infra::mail::inbox::Inbox;
use eba_rs_infra::notify::Signal;
use eba_rs_infra::pool::{PoolError, WorkerPool};
use eba_rs_infra::record::{dedup_key, DedupWindow, MemorySink, Record, RecordSink};
use eba_rs_infra::runtime::Runtime;
use eba_rs_infra::schema::{SchemaRegistry, TopicSchema};
use eba_rs_infra::time::clock::ManualClock;
use eba_rs_infra::Payload;

type Log = Rc<RefCell<Vec<String>>>;

fn log() -> Log {
    Rc::new(RefCell::new(Vec::new()))
}

fn payload(text: &str) -> Payload {
    Arc::new(text.to_string())
}

/// A `Clock` sharing one underlying `ManualClock` so the test can advance it
/// between `advance()` turns.
#[derive(Clone)]
struct SharedClock(Rc<ManualClock>);

impl eba_rs_infra::time::clock::Clock for SharedClock {
    fn now_ms(&self) -> u64 {
        self.0.now_ms()
    }
    fn arms_timer(&self) -> bool {
        self.0.arms_timer()
    }
}

fn advance(runtime: &mut Runtime, clock: &ManualClock, max_turns: usize, step_ms: u64) {
    for _ in 0..max_turns {
        if runtime.advance().is_err() {
            break;
        }
        clock.advance(step_ms);
    }
}

/// A sink shared with the test so durability can be observed after the `Core`
/// owns it.
struct SharedSink(Rc<RefCell<MemorySink>>);

impl RecordSink for SharedSink {
    fn append(&mut self, record: &Record) -> Result<(), BoxError> {
        self.0.borrow_mut().append(record)
    }
    fn flush(&mut self, seq: u64) -> Result<(), BoxError> {
        self.0.borrow_mut().flush(seq)
    }
    fn checkpoint(&mut self, seq: u64) -> Result<u64, BoxError> {
        self.0.borrow_mut().checkpoint(seq)
    }
    fn durable(&self) -> u64 {
        self.0.borrow().durable()
    }
}

// A single-actor host. `make_job` rebuilds the job from shared `hooks` so the
// same behaviour can be observed from the test.
struct Host {
    core: Rc<RefCell<Core>>,
    hooks: Rc<dyn JobHooks>,
}

impl JobHost for Host {
    fn core(&self) -> Rc<RefCell<Core>> {
        self.core.clone()
    }
    fn make_job(&self, root: Rc<Envelope>) -> Result<Job, BoxError> {
        Ok(Job::new(root, self.hooks.clone()))
    }
    fn on_rejected(&self, _root: Rc<Envelope>, _reason: String) -> Result<(), BoxError> {
        Ok(())
    }
}

fn build_host(
    actor: &str,
    patterns: &[&str],
    hooks: Rc<dyn JobHooks>,
    sink: Option<Box<dyn RecordSink>>,
) -> Rc<dyn JobHost> {
    let hooks_for_core = hooks.clone();
    let core = Core::new(CoreOptions {
        actor_id: actor_id(actor),
        receiver: Inbox::new(256).receiver(),
        patterns: patterns.iter().map(|item| (*item).to_string()).collect(),
        request_timeout_ms: None,
        queue_limit: CoreOptions::DEFAULT_QUEUE_LIMIT,
        active_limit: CoreOptions::DEFAULT_ACTIVE_LIMIT,
        work_budget: CoreOptions::DEFAULT_WORK_BUDGET,
        mailbox_limit: CoreOptions::DEFAULT_MAILBOX_LIMIT,
        make_job: Rc::new(move |root: Rc<Envelope>| {
            Ok(Job::new(root.clone(), hooks_for_core.clone()))
        }),
        on_rejected: Rc::new(|_, _| Ok(())),
        record_sink: sink,
    });
    Rc::new(Host {
        core: Rc::new(RefCell::new(core)),
        hooks,
    })
}

fn new_runtime(bus: Bus, clock: Rc<ManualClock>) -> Runtime {
    Runtime::new(
        bus,
        Rc::new(SharedClock(clock)),
        Rc::new(RefCell::new(Box::new(SeqIdGen::default()))),
    )
}

mod effect_resume {
    use super::*;

    struct TimerHooks {
        log: Log,
    }

    impl JobHooks for TimerHooks {
        fn begin(&self, ctx: &Context) -> Result<(), BoxError> {
            self.log.borrow_mut().push("begin".to_string());
            let lease: JobLease = ctx.hold(None, None)?;
            ctx.timer(60)?;
            ctx.tell("after", payload("post"))?;
            let _ = lease;
            Ok(())
        }
    }

    struct ObserverHooks {
        log: Log,
    }

    impl JobHooks for ObserverHooks {
        fn begin(&self, _ctx: &Context) -> Result<(), BoxError> {
            self.log.borrow_mut().push("obs".to_string());
            Ok(())
        }
    }

    #[test]
    fn timer_post_tell() {
        let observer_log = log();
        let timer_log = log();
        let clock = Rc::new(ManualClock::new(0));
        let mut runtime = new_runtime(Bus::default(), clock.clone());
        let observer = build_host(
            "observer",
            &["after"],
            Rc::new(ObserverHooks {
                log: observer_log.clone(),
            }),
            None,
        );
        let timer = build_host(
            "timer",
            &["tick"],
            Rc::new(TimerHooks {
                log: timer_log.clone(),
            }),
            None,
        );
        runtime.add(observer).unwrap();
        runtime.add(timer).unwrap();

        runtime
            .ask("tick", payload("now"), actor_id("caller"), None)
            .unwrap();
        advance(&mut runtime, &clock, 3, 0);

        let schedule = runtime.schedule();
        assert_eq!(
            schedule.next_deadline_ms,
            Some(60),
            "timer must arm the agenda"
        );
        assert!(timer_log.borrow().contains(&"begin".to_string()));

        clock.advance(60);
        advance(&mut runtime, &clock, 4, 0);
        assert!(
            observer_log.borrow().iter().any(|item| item == "obs"),
            "post-timer tell must reach the observer"
        );
    }
}

mod lease_revoke {
    use super::*;

    struct WorkerHooks {
        log: Log,
    }

    impl JobHooks for WorkerHooks {
        fn begin(&self, ctx: &Context) -> Result<(), BoxError> {
            self.log.borrow_mut().push("begin".to_string());
            let lease: JobLease = ctx.hold(None, None)?;
            ctx.timer(40)?;
            let _ = lease;
            Ok(())
        }
        fn on_resume(&self, ctx: &Context) -> Result<Vec<Effect>, BoxError> {
            self.log.borrow_mut().push("resumed".to_string());
            ctx.finish(Some(ok_payload(payload("done"))))?;
            Ok(Vec::new())
        }
    }

    #[test]
    fn hold_resume_finish() {
        let log = log();
        let clock = Rc::new(ManualClock::new(0));
        let mut runtime = new_runtime(Bus::default(), clock.clone());
        let host = build_host(
            "worker",
            &["task"],
            Rc::new(WorkerHooks { log: log.clone() }),
            None,
        );
        runtime.add(host).unwrap();

        let pending = runtime
            .ask("task", payload("now"), actor_id("caller"), None)
            .unwrap();
        advance(&mut runtime, &clock, 3, 0);
        assert!(log.borrow().contains(&"begin".to_string()));
        assert!(pending.poll().is_none(), "job still held");

        clock.advance(40);
        advance(&mut runtime, &clock, 60, 1);
        assert!(
            pending.poll().is_some(),
            "job must finish after timer + onResume"
        );
    }
}

mod record_ring {
    use super::*;

    struct RecorderHooks;

    impl JobHooks for RecorderHooks {
        fn begin(&self, ctx: &Context) -> Result<(), BoxError> {
            ctx.record("fact", payload("one"))?;
            ctx.record("fact", payload("two"))?;
            ctx.finish(Some(ok_payload(payload("done"))))?;
            Ok(())
        }
    }

    #[test]
    fn per_turn_drain_reaches_sink() {
        let shared = Rc::new(RefCell::new(MemorySink::new()));
        let clock = Rc::new(ManualClock::new(0));
        let mut runtime = new_runtime(Bus::default(), clock.clone());
        let host = build_host(
            "recorder",
            &["go"],
            Rc::new(RecorderHooks),
            Some(Box::new(SharedSink(shared.clone()))),
        );
        runtime.add(host).unwrap();

        let pending = runtime
            .ask("go", payload("now"), actor_id("caller"), None)
            .unwrap();
        advance(&mut runtime, &clock, 40, 1);
        // `settle_records` runs at the start of a turn, so durability lags the
        // last host step by one turn; a final advance flushes the remainder.
        runtime.advance().unwrap();
        assert!(pending.poll().is_some(), "job must finish");
        assert!(
            shared.borrow().durable() >= 2,
            "per-turn drain must reach the sink (durable={})",
            shared.borrow().durable()
        );
    }
}

mod record_log {
    use super::*;

    struct LoggerHooks {
        log: Log,
    }

    impl JobHooks for LoggerHooks {
        fn begin(&self, ctx: &Context) -> Result<(), BoxError> {
            self.log.borrow_mut().push("begin".to_string());
            ctx.record("audit", payload("fact"))?;
            ctx.barrier(None)?;
            Ok(())
        }
        fn on_resume(&self, ctx: &Context) -> Result<Vec<Effect>, BoxError> {
            self.log.borrow_mut().push("onResume".to_string());
            ctx.finish(Some(ok_payload(payload("done"))))?;
            Ok(Vec::new())
        }
    }

    #[test]
    fn barrier_resumes_after_durable() {
        let log = log();
        let clock = Rc::new(ManualClock::new(0));
        let mut runtime = new_runtime(Bus::default(), clock.clone());
        let host = build_host(
            "logger",
            &["go"],
            Rc::new(LoggerHooks { log: log.clone() }),
            None,
        );
        runtime.add(host).unwrap();

        let pending = runtime
            .ask("go", payload("now"), actor_id("caller"), None)
            .unwrap();
        advance(&mut runtime, &clock, 40, 1);
        assert!(
            log.borrow().contains(&"onResume".to_string()),
            "barrier must resolve once the recorded fact is durable"
        );
        assert!(pending.poll().is_some(), "job must finish after resume");
    }
}

mod effect_host {
    use super::*;

    struct TellHooks;

    impl JobHooks for TellHooks {
        fn begin(&self, ctx: &Context) -> Result<(), BoxError> {
            ctx.tell("side", payload("seen"))?;
            ctx.record("fact", payload("one"))?;
            ctx.finish(Some(ok_payload(payload("done"))))?;
            Ok(())
        }
    }

    struct ObserverHooks {
        log: Log,
    }

    impl JobHooks for ObserverHooks {
        fn begin(&self, _ctx: &Context) -> Result<(), BoxError> {
            self.log.borrow_mut().push("obs".to_string());
            Ok(())
        }
    }

    #[test]
    fn tell_and_record_through_host() {
        let observer_log = log();
        let shared = Rc::new(RefCell::new(MemorySink::new()));
        let clock = Rc::new(ManualClock::new(0));
        let mut runtime = new_runtime(Bus::default(), clock.clone());
        let tell = build_host(
            "host",
            &["go"],
            Rc::new(TellHooks),
            Some(Box::new(SharedSink(shared.clone()))),
        );
        let observer = build_host(
            "observer",
            &["side"],
            Rc::new(ObserverHooks {
                log: observer_log.clone(),
            }),
            None,
        );
        runtime.add(tell).unwrap();
        runtime.add(observer).unwrap();

        let pending = runtime
            .ask("go", payload("now"), actor_id("caller"), None)
            .unwrap();
        advance(&mut runtime, &clock, 40, 1);
        runtime.advance().unwrap();
        assert!(
            observer_log.borrow().iter().any(|item| item == "obs"),
            "tell effect must reach the observer"
        );
        assert!(
            shared.borrow().durable() >= 1,
            "record effect must reach the sink"
        );
        assert!(pending.poll().is_some(), "job must finish");
    }
}

mod lease_deadline {
    use super::*;

    struct DeadlineHooks;

    impl JobHooks for DeadlineHooks {
        fn begin(&self, ctx: &Context) -> Result<(), BoxError> {
            let lease: JobLease = ctx.hold(Some(5), None)?;
            let _ = lease;
            Ok(())
        }
    }

    #[test]
    fn deadline_revokes_held_job() {
        let clock = Rc::new(ManualClock::new(0));
        let mut runtime = new_runtime(Bus::default(), clock.clone());
        let host = build_host("worker", &["task"], Rc::new(DeadlineHooks), None);
        runtime.add(host).unwrap();

        let pending = runtime
            .ask("task", payload("now"), actor_id("caller"), None)
            .unwrap();
        advance(&mut runtime, &clock, 3, 0);
        clock.advance(4);
        advance(&mut runtime, &clock, 1, 0);
        assert!(pending.poll().is_none(), "still held at t=4 (deadline 5)");
        clock.advance(1);
        advance(&mut runtime, &clock, 10, 0);
        assert!(
            pending.poll().is_some(),
            "deadline must revoke the held job at t=5"
        );
    }
}

mod lease_budget {
    use super::*;

    #[test]
    fn budget_tracks_remaining() {
        let lease = JobLease::with_options(None, Some(10));
        assert!(lease.consume(4), "budget 10 covers 4");
        assert_eq!(lease.remaining_ms(), Some(6));
        assert!(!lease.consume(7), "budget exhausted");
        assert_eq!(lease.remaining_ms(), Some(-1));
    }
}

mod schema_version {
    use super::*;

    #[test]
    fn registry_check_and_empty_topic() {
        let mut registry = SchemaRegistry::new();
        registry
            .register(
                "fact",
                TopicSchema {
                    version: 2,
                    validate: Box::new(|_payload| Ok(())),
                    migrate: None,
                },
            )
            .unwrap();
        assert_eq!(registry.check("fact", &payload("x")).unwrap(), 2);
        assert_eq!(registry.version("other"), 1, "unknown topic defaults to 1");
        let error = registry
            .register(
                "",
                TopicSchema {
                    version: 1,
                    validate: Box::new(|_payload| Ok(())),
                    migrate: None,
                },
            )
            .unwrap_err();
        assert!(
            error.to_string().contains("non-empty"),
            "empty topic must be rejected: {error}"
        );
    }
}

mod dedup_window {
    use super::*;

    #[test]
    fn capacity_bounded_window() {
        let mut window = DedupWindow::new(2);
        assert!(window.accept("a"));
        assert!(!window.accept("a"));
        let key = dedup_key("from", "id");
        assert!(window.accept(&key));
        window.accept("c"); // evicts "a"
        assert!(!window.has("a"), "oldest evicted at capacity");
        assert!(window.has("c"));
        assert!(window.has(&key));
    }
}

mod pool_bounds {
    use super::*;

    #[test]
    fn queue_bound_rejects_overflow() {
        let pool = WorkerPool::new(1, 1);
        let held = std::rc::Rc::new(core::cell::RefCell::new(None));
        let slot = held.clone();
        pool.submit(false, move |token| {
            *slot.borrow_mut() = Some(token);
        })
        .unwrap();
        pool.submit(false, |_token| {}).unwrap();
        let error = pool
            .submit(false, |_token| {})
            .expect_err("third task must overflow the queue");
        assert_eq!(error, PoolError::QueueFull);
        assert_eq!(pool.queued_count(), 1);
        if let Some(token) = held.borrow_mut().take() {
            token.complete();
        }
        pool.close();
        assert!(pool.is_closed());
        assert_eq!(
            pool.submit(false, |_token| {}).unwrap_err(),
            PoolError::Closed
        );
    }
}

mod mailbox_busy {
    use super::*;
    use eba_rs_infra::errors::is_mailbox_busy;

    #[test]
    fn over_limit_send_is_refused() {
        let bus = Bus::default();
        let inbox = Inbox::new(1);
        bus.subscribe("mbox", &inbox).unwrap();

        let mut id_gen = SeqIdGen::default();
        let signal = Rc::new(Signal::new());
        let mut mailbox = Mailbox::new(1, signal);

        // First send publishes directly into the empty inbox — mailbox stays empty.
        let first = Rc::new(
            make_envelope(MakeEnvelope {
                topic: "mbox".into(),
                payload: payload("a"),
                from: actor_id("sender"),
                cause: None,
                id: Some(id_gen.next_envelope_id()),
                kind: "event".into(),
                reply_to: None,
                correlation_id: None,
            })
            .unwrap(),
        );
        mailbox.send(&bus, first).unwrap();

        // Second send cannot publish (inbox now full) and parks in the mailbox → full.
        let second = Rc::new(
            make_envelope(MakeEnvelope {
                topic: "mbox".into(),
                payload: payload("b"),
                from: actor_id("sender"),
                cause: None,
                id: Some(id_gen.next_envelope_id()),
                kind: "event".into(),
                reply_to: None,
                correlation_id: None,
            })
            .unwrap(),
        );
        mailbox.send(&bus, second).unwrap();
        assert!(!mailbox.has_room(), "mailbox is full");

        // Third send: still cannot publish, mailbox full → refused.
        let third = Rc::new(
            make_envelope(MakeEnvelope {
                topic: "mbox".into(),
                payload: payload("c"),
                from: actor_id("sender"),
                cause: None,
                id: Some(id_gen.next_envelope_id()),
                kind: "event".into(),
                reply_to: None,
                correlation_id: None,
            })
            .unwrap(),
        );
        let error = mailbox
            .send(&bus, third)
            .expect_err("third send must be refused");
        assert!(is_mailbox_busy(&error), "refusal must be a mailbox-busy");
    }
}

mod mailbox_space {
    use super::*;
    use eba_rs_infra::collections::IndexedMap;
    use eba_rs_infra::notify::Interest;

    #[test]
    fn freeing_a_slot_notifies_space() {
        let bus = Bus::default();
        let inbox = Inbox::new(1);
        bus.subscribe("mbox", &inbox).unwrap();
        // Fill the single-slot inbox so the next send cannot publish directly.
        let mut id_gen = SeqIdGen::default();
        let filler = Rc::new(
            make_envelope(MakeEnvelope {
                topic: "mbox".into(),
                payload: payload("fill"),
                from: actor_id("sender"),
                cause: None,
                id: Some(id_gen.next_envelope_id()),
                kind: "event".into(),
                reply_to: None,
                correlation_id: None,
            })
            .unwrap(),
        );
        bus.publish(&filler).unwrap();

        let signal = Rc::new(Signal::new());
        let mut mailbox = Mailbox::new(1, Rc::clone(&signal));

        let space_fired = Rc::new(core::cell::Cell::new(false));
        let candidates = Rc::new(RefCell::new(IndexedMap::<String, ()>::new()));
        let interest = Rc::new(Interest::new(signal, candidates, "mbox".to_string()));
        let fired = space_fired.clone();
        interest.set_on_notify(Rc::new(move || {
            fired.set(true);
        }));
        mailbox.watch_space(&interest);

        let parked = Rc::new(
            make_envelope(MakeEnvelope {
                topic: "mbox".into(),
                payload: payload("park"),
                from: actor_id("sender"),
                cause: None,
                id: Some(id_gen.next_envelope_id()),
                kind: "event".into(),
                reply_to: None,
                correlation_id: None,
            })
            .unwrap(),
        );
        // Cannot publish (inbox full) and cannot queue past the limit, so it
        // parks as the sole item: mailbox full, no space notice yet.
        mailbox.send(&bus, parked).unwrap();
        assert!(
            !space_fired.get() && !mailbox.has_room(),
            "no room, no notice yet"
        );

        let drained = mailbox.drain();
        assert_eq!(drained.len(), 1);
        assert!(
            space_fired.get() && mailbox.has_room(),
            "space must be noticed once the slot frees"
        );
    }
}
