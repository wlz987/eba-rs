use std::cell::RefCell;
use std::rc::Rc;

use eba_rs_infra::envelope::idgen::{IdGen, SeqIdGen};
use eba_rs_infra::envelope::ids::actor_id;
use eba_rs_infra::errors::BoxError;
use eba_rs_infra::job::core::{Core, CoreOptions, HostState, MakeJob};
use eba_rs_infra::job::host::JobHost;
use eba_rs_infra::job::job::{Job, JobHooks, SharedIdGen};
use eba_rs_infra::mail::{Bus, Inbox, Receiver};
use eba_rs_infra::runtime::Runtime;
use eba_rs_infra::time::ManualClock;

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
    let mut runtime = Runtime::new(Bus::new(), Rc::new(ManualClock::new(0)), ids);
    let a = host(
        "a",
        Inbox::new(8).receiver(),
        vec![],
        Rc::new(|root| Ok(Job::new(root, Rc::new(SilentHooks)))),
    );
    let b = host(
        "b",
        Inbox::new(8).receiver(),
        vec![],
        Rc::new(|root| Ok(Job::new(root, Rc::new(SilentHooks)))),
    );
    runtime.add(a.clone())?;
    runtime.add(b.clone())?;
    runtime.shutdown(Some("a"))?;
    if a.state() != HostState::Draining || b.state() != HostState::Running {
        return Err("selective drain leaked".into());
    }
    runtime.close(None)?;
    println!("drain: {:?} {:?}", a.state(), b.state());
    Ok(())
}
