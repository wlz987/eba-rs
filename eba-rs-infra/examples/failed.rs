use std::cell::RefCell;
use std::rc::Rc;

use eba_rs_infra::envelope::idgen::{IdGen, SeqIdGen};
use eba_rs_infra::envelope::ids::actor_id;
use eba_rs_infra::envelope::result::ResultBody;
use eba_rs_infra::errors::{is_settled, BoxError};
use eba_rs_infra::job::caller::{settled, CallOptions, Caller};
use eba_rs_infra::job::core::{Core, CoreOptions, MakeJob};
use eba_rs_infra::job::host::JobHost;
use eba_rs_infra::job::job::{Context, Job, JobHooks, SharedIdGen};
use eba_rs_infra::mail::{Bus, Inbox, Receiver};
use eba_rs_infra::runtime::Runtime;
use eba_rs_infra::time::ManualClock;
use eba_rs_infra::Payload;

struct FailHooks;
impl JobHooks for FailHooks {
    fn begin(&self, _ctx: &Context) -> Result<(), BoxError> {
        Err("boom".into())
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
    fn make_job(&self, root: Rc<eba_rs_infra::envelope::Envelope>) -> Result<Job, BoxError> {
        (self.make_job)(root)
    }
    fn on_rejected(
        &self,
        _root: Rc<eba_rs_infra::envelope::Envelope>,
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

fn main() -> Result<(), BoxError> {
    let ids = shared_ids();
    let mut runtime = Runtime::new(Bus::new(), Rc::new(ManualClock::new(0)), ids.clone());
    runtime.add(host(
        "fail",
        Inbox::new(8).receiver(),
        vec!["fail.work".into()],
        Rc::new(|root| Ok(Job::new(root, Rc::new(FailHooks)))),
    ))?;
    let caller = Caller::new(Inbox::new(8).receiver(), ids, 256);
    runtime.add(caller.clone())?;
    let envelope = caller.admit(CallOptions {
        topic: "fail.work".into(),
        payload: std::sync::Arc::new(1_u64) as Payload,
        from: actor_id("fail"),
        timeout_ms: None,
    })?;
    for _ in 0..40 {
        if settled(&envelope).is_some() {
            break;
        }
        match runtime.advance() {
            Ok(true) => {}
            Ok(false) => break,
            Err(error) if is_settled(&error) => {}
            Err(error) => return Err(error),
        }
    }
    match settled(&envelope).and_then(|completion| completion.body) {
        Some(ResultBody::Err { error, .. }) if error == "job_failed" => {
            println!("failed: {error}");
            Ok(())
        }
        other => Err(format!("expected job_failed, got {other:?}").into()),
    }
}
