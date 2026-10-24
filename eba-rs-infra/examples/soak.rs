use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use eba_rs_infra::envelope::envelope::Envelope;
use eba_rs_infra::envelope::idgen::{IdGen, SeqIdGen};
use eba_rs_infra::envelope::ids::actor_id;
use eba_rs_infra::envelope::result::{ok_payload, ResultBody};
use eba_rs_infra::errors::BoxError;
use eba_rs_infra::job::caller::{CallOptions, Caller};
use eba_rs_infra::job::job::{Context, JobHooks, SharedIdGen};
use eba_rs_infra::job::Service;
use eba_rs_infra::mail::{Bus, Inbox};
use eba_rs_infra::runtime::Runtime;
use eba_rs_infra::time::ManualClock;
use eba_rs_infra::Payload;

struct Echo;

impl JobHooks for Echo {
    fn begin(&self, ctx: &Context) -> Result<(), BoxError> {
        ctx.reply(ok_payload(Arc::new(1u64) as Payload))
    }
}

struct Hold;

impl JobHooks for Hold {
    fn begin(&self, ctx: &Context) -> Result<(), BoxError> {
        ctx.hold()?;
        Ok(())
    }
}

struct Bad;

impl JobHooks for Bad {
    fn begin(&self, ctx: &Context) -> Result<(), BoxError> {
        ctx.request("inner", "hold", Arc::new(1u64) as Payload, None)?;
        Ok(())
    }

    fn on_finished(&self, _body: Option<&ResultBody>) -> Result<(), BoxError> {
        Err("cleanup".into())
    }
}

fn next(seed: &mut u64) -> usize {
    *seed = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    ((*seed >> 33) as usize) % 3
}

fn main() -> Result<(), BoxError> {
    let mut seed = 7u64;
    let mut cancel_errors = 0usize;
    for wave in 0..100 {
        let ids: SharedIdGen = Rc::new(RefCell::new(Box::new(SeqIdGen::new(1)) as Box<dyn IdGen>));
        let mut runtime = Runtime::new(Bus::new(), Rc::new(ManualClock::new(0)), ids.clone());
        runtime.add(Service::mount("echo", 64, &["echo"], |_| Echo)?)?;
        runtime.add(Service::mount("hold", 64, &["hold"], |_| Hold)?)?;
        runtime.add(Service::mount("bad", 64, &["bad"], |_| Bad)?)?;
        let caller = Caller::new(Inbox::new(256).receiver(), ids, 256);
        runtime.add(caller.clone())?;
        let mut roots: Vec<(&str, Rc<Envelope>)> = Vec::new();
        for index in 0..20u64 {
            let topic = match next(&mut seed) {
                0 => "echo",
                1 => "hold",
                _ => "bad",
            };
            let root = caller.admit(CallOptions {
                topic: topic.into(),
                payload: Arc::new(index) as Payload,
                from: actor_id("soak"),
                timeout_ms: None,
            })?;
            roots.push((topic, root));
        }
        for _ in 0..3 {
            runtime.advance()?;
        }
        for (topic, root) in &roots {
            if *topic == "echo" {
                continue;
            }
            let _ = caller.cancel(&root.header.id)?;
        }
        for _ in 0..12 {
            runtime.advance()?;
        }
        let stats = runtime.stats();
        if stats.active != 0 || stats.held != 0 || stats.pending != 0 || stats.queued != 0 {
            return Err(format!(
                "soak leak wave {wave}: active={} held={} pending={} queued={}",
                stats.active, stats.held, stats.pending, stats.queued
            )
            .into());
        }
        for (actor, detail) in &stats.per_host {
            if detail.active != 0 || detail.held != 0 || detail.pending != 0 || detail.queued != 0 {
                return Err(format!(
                    "soak host {actor} leak wave {wave}: active={} held={} pending={} queued={}",
                    detail.active, detail.held, detail.pending, detail.queued
                )
                .into());
            }
        }
        cancel_errors += stats.cancel_errors;
        runtime.close(None)?;
    }
    if cancel_errors == 0 {
        return Err("soak did not exercise cancel_errors".into());
    }
    println!("soak ok: 100 waves x 20 asks, {cancel_errors} contained hook errors");
    Ok(())
}
