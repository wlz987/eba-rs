use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use eba_rs::envelope::idgen::{IdGen, SeqIdGen};
use eba_rs::envelope::ids::actor_id;
use eba_rs::envelope::result::{ok_payload, result_error, result_value, Completion};
use eba_rs::errors::BoxError;
use eba_rs::job::caller::Outcome;
use eba_rs::job::{Context, JobHooks, Service, SharedIdGen};
use eba_rs::mail::Bus;
use eba_rs::runtime::Runtime;
use eba_rs::time::ManualClock;
use eba_rs::Payload;

struct Echo;

impl JobHooks for Echo {
    fn begin(&self, ctx: &Context) -> Result<(), BoxError> {
        let value = ctx
            .root()
            .payload
            .downcast_ref::<i64>()
            .copied()
            .unwrap_or(0);
        ctx.reply(ok_payload(Arc::new(value) as Payload))
    }
}

struct Hold;

impl JobHooks for Hold {
    fn begin(&self, ctx: &Context) -> Result<(), BoxError> {
        ctx.hold()?;
        Ok(())
    }
}

fn main() -> Result<(), BoxError> {
    let ids: SharedIdGen = Rc::new(RefCell::new(Box::new(SeqIdGen::new(1)) as Box<dyn IdGen>));
    let mut runtime = Runtime::new(Bus::new(), Rc::new(ManualClock::new(0)), ids);
    runtime.add(Service::mount("echo", 32, &["echo"], |_root| Echo)?)?;
    runtime.add(Service::mount("hold", 8, &["hold"], |_root| Hold)?)?;

    let seen: Rc<RefCell<Vec<Completion>>> = Rc::new(RefCell::new(Vec::new()));
    let sink = Rc::clone(&seen);
    runtime.ask_with(
        "echo",
        Arc::new(7i64) as Payload,
        actor_id("handoff"),
        None,
        Box::new(move |outcome| {
            if let Outcome::Settled(completion) = outcome {
                sink.borrow_mut().push(completion);
            }
        }),
    )?;
    for _ in 0..12 {
        runtime.advance()?;
    }

    let seen = seen.borrow();
    assert_eq!(seen.len(), 1);
    let value = seen[0]
        .body
        .as_ref()
        .and_then(result_value)
        .and_then(|payload| payload.downcast_ref::<i64>())
        .copied();
    assert_eq!(value, Some(7));

    let batch: Rc<RefCell<Vec<Completion>>> = Rc::new(RefCell::new(Vec::new()));
    for value in 0..9i64 {
        let sink = Rc::clone(&batch);
        runtime.ask_with(
            "echo",
            Arc::new(value) as Payload,
            actor_id("handoff"),
            None,
            Box::new(move |outcome| {
                if let Outcome::Settled(completion) = outcome {
                    sink.borrow_mut().push(completion);
                }
            }),
        )?;
    }
    for _ in 0..24 {
        runtime.advance()?;
    }
    let mut values: Vec<i64> = batch
        .borrow()
        .iter()
        .map(|completion| {
            completion
                .body
                .as_ref()
                .and_then(result_value)
                .and_then(|payload| payload.downcast_ref::<i64>())
                .copied()
                .unwrap_or(-1)
        })
        .collect();
    assert_eq!(values.len(), 9);
    values.sort_unstable();
    assert_eq!(values, (0..9).collect::<Vec<i64>>());

    let closed: Rc<RefCell<Vec<Completion>>> = Rc::new(RefCell::new(Vec::new()));
    let sink = Rc::clone(&closed);
    runtime.ask_with(
        "hold",
        Arc::new(0i64) as Payload,
        actor_id("handoff"),
        None,
        Box::new(move |outcome| {
            if let Outcome::Settled(completion) = outcome {
                sink.borrow_mut().push(completion);
            }
        }),
    )?;
    for _ in 0..4 {
        runtime.advance()?;
    }
    runtime.close(None)?;
    let closed = closed.borrow();
    assert_eq!(closed.len(), 1);
    let error = closed[0].body.as_ref().and_then(result_error);
    assert_eq!(error, Some("host_draining"));
    println!("handoff ok");
    Ok(())
}
