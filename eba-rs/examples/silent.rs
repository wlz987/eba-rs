use std::cell::RefCell;
use std::rc::Rc;

use eba_rs::drive::Drive;
use eba_rs::envelope::idgen::{IdGen, SeqIdGen};
use eba_rs::envelope::ids::actor_id;
use eba_rs::envelope::result::ResultBody;
use eba_rs::errors::{BoxError, BusError};
use eba_rs::job::caller::{CallOptions, Caller};
use eba_rs::job::core::{Core, CoreOptions, MakeJob};
use eba_rs::job::host::JobHost;
use eba_rs::job::job::{Job, JobHooks, SharedIdGen};
use eba_rs::mail::{Bus, Inbox, Receiver};
use eba_rs::runtime::Runtime;
use eba_rs::time::ManualClock;
use eba_rs::Payload;

struct SilentHooks;
impl JobHooks for SilentHooks {}

struct Host {
    core: Rc<RefCell<Core>>,
    make_job: MakeJob,
}

impl JobHost for Host {
    fn core(&self) -> Rc<RefCell<Core>> {
        self.core.clone()
    }
    fn make_job(&self, root: Rc<eba_rs::envelope::Envelope>) -> Result<Job, BoxError> {
        (self.make_job)(root)
    }
    fn on_rejected(
        &self,
        _root: Rc<eba_rs::envelope::Envelope>,
        _reason: String,
    ) -> Result<(), BoxError> {
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
    Rc::new(Host { core, make_job })
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
                "silent",
                Inbox::new(8).receiver(),
                vec!["silent.work".into()],
                Rc::new(|root| Ok(Job::new(root, Rc::new(SilentHooks)))),
            ))?;
            let caller = Caller::new(Inbox::new(8).receiver(), ids, 256);
            let drive = Drive::new(runtime);
            drive.start();
            let pending = drive.open(
                caller,
                CallOptions {
                    topic: "silent.work".into(),
                    payload: std::sync::Arc::new(1_u64) as Payload,
                    from: actor_id("silent"),
                    timeout_ms: None,
                },
            )?;
            let completion = pending
                .ready
                .await
                .map_err(|_| Box::new(BusError("pending dropped".into())) as BoxError)??;
            match completion.body {
                Some(ResultBody::Err { error, .. }) if error == "job_silent" => {
                    println!("silent: {error}");
                    drive.close().await?;
                    Ok(())
                }
                other => Err(format!("expected job_silent, got {other:?}").into()),
            }
        })
        .await
}
