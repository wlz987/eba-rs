//! `docs/bench.md` 记读数；不同机器/负载的读数不横向比。

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use eba_rs::drive::Drive;
use eba_rs::envelope::idgen::{IdGen, SeqIdGen};
use eba_rs::envelope::ids::actor_id;
use eba_rs::envelope::result::ok_payload;
use eba_rs::envelope::Envelope;
use eba_rs::errors::BoxError;
use eba_rs::job::caller::CallOptions;
use eba_rs::job::caller::Caller;
use eba_rs::job::core::{Core, CoreOptions, MakeJob};
use eba_rs::job::host::JobHost;
use eba_rs::job::job::{Context, Job, JobHooks, SharedIdGen};
use eba_rs::mail::inbox::Receiver;
use eba_rs::mail::{Bus, Inbox};
use eba_rs::runtime::Runtime;
use eba_rs::time::ManualClock;
use eba_rs::Payload;

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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let ids = shared_ids();
            let mut runtime = Runtime::new(Bus::new(), Rc::new(ManualClock::new(0)), ids.clone());
            runtime.add(host(
                "echo",
                Inbox::new(64).receiver(),
                vec!["echo.work".into()],
                Rc::new(|root| Ok(Job::new(root, Rc::new(EchoJob)))),
            ))?;
            let caller = Caller::new(Inbox::new(64).receiver(), ids, 256);
            let drive = Drive::new(runtime);
            drive.start();

            for _ in 0..50 {
                let pending = drive.open(
                    Rc::clone(&caller),
                    CallOptions {
                        topic: "echo.work".into(),
                        payload: Arc::new(0_u64) as Payload,
                        from: actor_id("bench"),
                        timeout_ms: None,
                    },
                )?;
                let _ = pending.ready.await??;
            }

            let total = 5000_u64;
            let window = 64_u64;
            let started = Instant::now();
            let mut done = 0_u64;
            while done < total {
                let batch = window.min(total - done);
                let mut handles = Vec::new();
                for i in 0..batch {
                    let pending = drive.open(
                        Rc::clone(&caller),
                        CallOptions {
                            topic: "echo.work".into(),
                            payload: Arc::new(done + i) as Payload,
                            from: actor_id("bench"),
                            timeout_ms: None,
                        },
                    )?;
                    let ready = pending.ready;
                    handles.push(tokio::task::spawn_local(ready));
                }
                for handle in handles {
                    let _ = handle.await??;
                }
                done += batch;
            }
            let elapsed = started.elapsed().as_secs_f64();
            drive.close().await?;
            println!(
                "{} messages in {:.3}s = {:.0} msg/s",
                total,
                elapsed,
                total as f64 / elapsed
            );
            Ok(())
        })
        .await
}
