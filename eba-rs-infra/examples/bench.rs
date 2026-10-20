//! 读数只量核心消息路径，不是 Drive 往返，`docs/bench.md` 把这一点写清楚。

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use eba_rs_infra::envelope::idgen::{IdGen, SeqIdGen};
use eba_rs_infra::envelope::ids::actor_id;
use eba_rs_infra::envelope::result::ok_payload;
use eba_rs_infra::envelope::Envelope;
use eba_rs_infra::errors::BoxError;
use eba_rs_infra::job::caller::{settled, CallOptions, Caller};
use eba_rs_infra::job::core::{Core, CoreOptions, MakeJob};
use eba_rs_infra::job::host::JobHost;
use eba_rs_infra::job::job::{Context, Job, JobHooks, SharedIdGen};
use eba_rs_infra::mail::inbox::Receiver;
use eba_rs_infra::mail::{Bus, Inbox};
use eba_rs_infra::runtime::Runtime;
use eba_rs_infra::time::ManualClock;
use eba_rs_infra::Payload;

struct EchoJob;

impl JobHooks for EchoJob {
    fn begin(&self, ctx: &Context) -> Result<(), BoxError> {
        ctx.finish(Some(ok_payload(Arc::new(String::from("ok")) as Payload)))?;
        Ok(())
    }
}

struct Host {
    core: Rc<RefCell<Core>>,
    make_job: MakeJob,
}

impl JobHost for Host {
    fn core(&self) -> Rc<RefCell<Core>> {
        self.core.clone()
    }

    fn make_job(&self, root: Rc<Envelope>) -> Result<Job, BoxError> {
        (self.make_job)(root)
    }

    fn on_rejected(&self, _root: Rc<Envelope>, _reason: String) -> Result<(), BoxError> {
        Ok(())
    }
}

fn host(
    actor: &str,
    receiver: Receiver,
    patterns: Vec<String>,
    make_job: MakeJob,
) -> Rc<dyn JobHost> {
    let core = Core::new(CoreOptions {
        actor_id: actor_id(actor),
        receiver,
        patterns,
        request_timeout_ms: None,
        queue_limit: 512,
        active_limit: 256,
        work_budget: 64,
        outbox_limit: 512,
        make_job: make_job.clone(),
        on_rejected: Rc::new(|_, _| Ok(())),
    });
    let core = Rc::new(RefCell::new(core));
    Rc::new(Host {
        core: core.clone(),
        make_job,
    })
}

fn shared_ids() -> SharedIdGen {
    Rc::new(RefCell::new(Box::new(SeqIdGen::new(1)) as Box<dyn IdGen>))
}

fn main() -> Result<(), BoxError> {
    let ids = shared_ids();
    let mut runtime = Runtime::new(Bus::new(), Rc::new(ManualClock::new(0)), ids.clone());
    runtime.add(host(
        "echo",
        Inbox::new(64).receiver(),
        vec!["echo.work".into()],
        Rc::new(|root| Ok(Job::new(root, Rc::new(EchoJob)))),
    ))?;
    let caller = Caller::new(Inbox::new(64).receiver(), ids, 256);
    runtime.add(caller.clone())?;

    for _ in 0..50 {
        let envelope = caller.admit(CallOptions {
            topic: "echo.work".into(),
            payload: Arc::new(0_u64) as Payload,
            from: actor_id("bench"),
            timeout_ms: None,
        })?;
        while settled(&envelope).is_none() {
            if !runtime.advance()? {
                break;
            }
        }
    }

    let total = 1000_u64;
    let started = Instant::now();
    for i in 0..total {
        let envelope = caller.admit(CallOptions {
            topic: "echo.work".into(),
            payload: Arc::new(i) as Payload,
            from: actor_id("bench"),
            timeout_ms: None,
        })?;
        while settled(&envelope).is_none() {
            if !runtime.advance()? {
                break;
            }
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "{} messages in {:.3}s = {:.0} msg/s",
        total,
        elapsed,
        total as f64 / elapsed
    );
    Ok(())
}
