use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::envelope::ids::actor_id;
use crate::envelope::topic::split_topic;
use crate::envelope::{make_envelope, Envelope, EnvelopeId, MakeEnvelope};
use crate::errors::{BackpressureError, BoxError, BusError, MailboxFull, REENTRANT};
pub use crate::job::call::{settle, settled, Call, Outcome, Pay};
use crate::job::core::{Core, CoreOptions, HostState, MakeJob, OnRejected};
use crate::job::host::JobHost;
use crate::job::job::SharedIdGen;
use crate::mail::inbox::{can_enqueue, enqueue, mark_read, reply_topic, Receiver};
use crate::time::numbers::nonnegative_int;
use crate::Payload;

mod host;

use self::host::{end, mark};

const ADMIT: &str = "sys.admit";

pub struct Caller {
    core: Rc<RefCell<Core>>,
    pub id_gen: SharedIdGen,
}

pub struct CallOptions {
    pub topic: String,
    pub payload: Payload,
    pub from: crate::envelope::ids::ActorId,
    pub timeout_ms: Option<u64>,
}

impl Caller {
    pub fn new(receiver: Receiver, id_gen: SharedIdGen, active_limit: usize) -> Rc<Caller> {
        Self::with_actor(receiver, id_gen, active_limit, actor_id("caller"))
    }

    pub fn with_actor(
        receiver: Receiver,
        id_gen: SharedIdGen,
        active_limit: usize,
        actor_id: crate::envelope::ids::ActorId,
    ) -> Rc<Caller> {
        Rc::new_cyclic(|weak| {
            let make_job: MakeJob = {
                let weak = weak.clone();
                Rc::new(move |root| {
                    let caller: Rc<Caller> = weak.upgrade().expect("caller dropped");
                    caller.make_job(root)
                })
            };
            let on_rejected: OnRejected = {
                let weak = weak.clone();
                Rc::new(move |root, reason| match weak.upgrade() {
                    Some(caller) => caller.on_rejected(root, reason),
                    None => Ok(()),
                })
            };
            let core = Core::new(CoreOptions {
                actor_id,
                receiver,
                patterns: vec![],
                request_timeout_ms: None,
                queue_limit: CoreOptions::DEFAULT_QUEUE_LIMIT,
                active_limit,
                work_budget: CoreOptions::DEFAULT_WORK_BUDGET,
                outbox_limit: CoreOptions::DEFAULT_OUTBOX_LIMIT,
                make_job,
                on_rejected,
            });
            Caller {
                core: Rc::new(RefCell::new(core)),
                id_gen: id_gen.clone(),
            }
        })
    }

    pub fn admit(&self, options: CallOptions) -> Result<Rc<Envelope>, BoxError> {
        let core_ref = self.core.clone();
        let core = core_ref.borrow();
        if core.busy {
            return Err(Box::from(BusError(REENTRANT.into())));
        }
        let inbox = core.sub.inbox.clone();
        if inbox.is_closed() {
            return Err(Box::from(BusError("inbox closed".into())));
        }
        if core.state == HostState::Draining {
            return Err(Box::from(BusError("host draining".into())));
        }
        if core.state != HostState::Running {
            return Err(Box::from(BusError("host closing".into())));
        }
        if let Some(ms) = options.timeout_ms {
            nonnegative_int(ms, "timeout_ms");
        }
        split_topic(&options.topic)?;
        if self.crowded() {
            return Err(Box::from(BackpressureError("active_full".into())));
        }
        if !can_enqueue(&inbox, "request") {
            return Err(Box::from(MailboxFull("target inbox full".into())));
        }
        let id = self.id_gen.borrow_mut().next_envelope_id();
        let request = make_envelope(MakeEnvelope {
            topic: ADMIT.into(),
            payload: Arc::new(Call::new(
                options.topic,
                options.payload,
                options.timeout_ms,
            )),
            from: options.from,
            cause: None,
            id: Some(id),
            kind: "request".into(),
            reply_to: Some(reply_topic(&inbox)),
            correlation_id: None,
        })
        .map_err(|error| Box::new(error) as BoxError)?;
        let rc = Rc::new(request);
        if !enqueue(&inbox, rc.clone()) {
            return Err(Box::from(MailboxFull("target inbox full".into())));
        }
        mark_read(&inbox);
        Ok(rc)
    }

    pub fn cancel(&self, root_id: &EnvelopeId) -> Result<bool, BoxError> {
        let core_ref = self.core.clone();
        let job = {
            let core = core_ref.borrow();
            if core.busy {
                return Err(Box::from(BusError(REENTRANT.into())));
            }
            if core.state == HostState::Closing || core.state == HostState::Closed {
                return Err(Box::from(BusError("host closing".into())));
            }
            core.jobs.get(root_id).cloned()
        };
        if let Some(job) = job {
            return end(self, &mut core_ref.borrow_mut(), &job);
        }
        Ok(mark(self, root_id))
    }
}
