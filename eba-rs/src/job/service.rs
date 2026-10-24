use std::cell::RefCell;
use std::rc::Rc;

use crate::envelope::ids::actor_id;
use crate::envelope::Envelope;
use crate::errors::BoxError;
use crate::job::core::{Core, CoreOptions};
use crate::job::host::JobHost;
use crate::job::job::{Job, JobHooks};
use crate::mail::inbox::Inbox;

type Logic = Rc<dyn Fn(Rc<Envelope>) -> Rc<dyn JobHooks>>;

/// 宿主：一个 actor + 一组 pattern + 一个逻辑工厂。
/// `on_rejected` 取默认空实现；需要时直接实现 `JobHost`。
pub struct Service {
    core: Rc<RefCell<Core>>,
    make: Logic,
}

impl Service {
    pub fn mount<L: JobHooks + 'static>(
        actor: &str,
        capacity: usize,
        patterns: &[&str],
        make: impl Fn(Rc<Envelope>) -> L + 'static,
    ) -> Result<Rc<dyn JobHost>, BoxError> {
        Self::mount_with(
            actor,
            capacity,
            patterns,
            CoreOptions::DEFAULT_ACTIVE_LIMIT,
            make,
        )
    }

    pub fn mount_with<L: JobHooks + 'static>(
        actor: &str,
        capacity: usize,
        patterns: &[&str],
        active_limit: usize,
        make: impl Fn(Rc<Envelope>) -> L + 'static,
    ) -> Result<Rc<dyn JobHost>, BoxError> {
        let receiver = Inbox::new(capacity).receiver();
        let make: Logic = Rc::new(move |root| Rc::new(make(root)));
        let factory = make.clone();
        let core = Core::try_new(CoreOptions {
            actor_id: actor_id(actor),
            receiver,
            patterns: patterns.iter().map(|item| String::from(*item)).collect(),
            request_timeout_ms: None,
            queue_limit: CoreOptions::DEFAULT_QUEUE_LIMIT,
            active_limit,
            work_budget: CoreOptions::DEFAULT_WORK_BUDGET,
            outbox_limit: CoreOptions::DEFAULT_OUTBOX_LIMIT,
            make_job: Rc::new(move |root| Ok(Job::new(root.clone(), factory(root)))),
            on_rejected: Rc::new(|_, _| Ok(())),
        })?;
        Ok(Rc::new(Service {
            core: Rc::new(RefCell::new(core)),
            make,
        }))
    }
}

impl JobHost for Service {
    fn core(&self) -> Rc<RefCell<Core>> {
        self.core.clone()
    }

    fn make_job(&self, root: Rc<Envelope>) -> Result<Job, BoxError> {
        Ok(Job::new(root.clone(), (self.make)(root)))
    }

    fn on_rejected(&self, _root: Rc<Envelope>, _reason: String) -> Result<(), BoxError> {
        Ok(())
    }
}
