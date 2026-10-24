use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use crate::envelope::ids::ActorId;
use crate::envelope::{Envelope, EnvelopeId};
use crate::errors::{BoxError, BusError};
use crate::job::book::RequestBook;
use crate::job::job::Job;
use crate::job::outbox::Outbox;
use crate::mail::inbox::remove;
use crate::mail::inbox::Receiver;
use crate::mail::queue::Queue;
use crate::mail::subscriber::Subscriber;
use crate::notify::Signal;
use crate::pattern::parse_pattern;
use crate::time::numbers::{nonnegative_int, positive_int};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostState {
    Running,
    Draining,
    Closing,
    Closed,
}

pub type MakeJob = Rc<dyn Fn(Rc<Envelope>) -> Result<Job, BoxError>>;
pub type OnRejected = Rc<dyn Fn(Rc<Envelope>, String) -> Result<(), BoxError>>;

pub struct Core {
    pub make_job: MakeJob,
    pub on_rejected: OnRejected,
    pub active_limit: usize,
    pub work_budget: usize,
    pub timeout: Option<u64>,
    pub queue: Queue<Rc<Envelope>>,
    pub sub: Subscriber,
    pub signal: Rc<Signal>,
    pub outbox: Outbox,
    pub jobs: HashMap<EnvelopeId, Job>,
    pub book: RequestBook,
    pub phase: usize,
    pub busy: bool,
    pub state: HostState,
    pub rejected_count: usize,
    pub cancel_errors: usize,
    pub last_cancel_error: Option<String>,
    pub cancel_pending: HashSet<EnvelopeId>,
    pub abandoned: Option<Vec<Rc<Envelope>>>,
    pub pending: Option<Rc<Envelope>>,
    pub pending_job: Option<Job>,
    pub rest: Vec<(Rc<Envelope>, Option<Job>)>,
    pub again: std::collections::HashSet<EnvelopeId>,
    pub attempts: std::collections::HashMap<EnvelopeId, usize>,
}

pub struct CoreOptions {
    pub actor_id: ActorId,
    pub receiver: Receiver,
    pub patterns: Vec<String>,
    pub request_timeout_ms: Option<u64>,
    pub queue_limit: usize,
    pub active_limit: usize,
    pub work_budget: usize,
    pub outbox_limit: usize,
    pub make_job: MakeJob,
    pub on_rejected: OnRejected,
}

impl CoreOptions {
    pub const DEFAULT_QUEUE_LIMIT: usize = 512;
    pub const DEFAULT_ACTIVE_LIMIT: usize = 256;
    pub const DEFAULT_WORK_BUDGET: usize = 64;
    pub const DEFAULT_OUTBOX_LIMIT: usize = 512;
}

impl Core {
    pub fn try_new(options: CoreOptions) -> Result<Self, BoxError> {
        for item in &options.patterns {
            parse_pattern(item)?;
        }
        if options.active_limit < 1 {
            return Err(
                Box::new(BusError("active_limit must be a positive integer".into())) as BoxError,
            );
        }
        if options.work_budget < 1 {
            return Err(
                Box::new(BusError("work_budget must be a positive integer".into())) as BoxError,
            );
        }
        if options.outbox_limit < 1 {
            return Err(
                Box::new(BusError("outbox_limit must be a positive integer".into())) as BoxError,
            );
        }
        if options.queue_limit < 1 {
            return Err(
                Box::new(BusError("queue_limit must be a positive integer".into())) as BoxError,
            );
        }
        Ok(Self::new(options))
    }

    pub fn new(options: CoreOptions) -> Self {
        for item in &options.patterns {
            parse_pattern(item).expect("invalid pattern");
        }
        let active_limit = positive_int(options.active_limit, "active_limit");
        let work_budget = positive_int(options.work_budget, "work_budget");
        let timeout = options
            .request_timeout_ms
            .map(|v| nonnegative_int(v, "request_timeout_ms"));
        let outbox_limit = positive_int(options.outbox_limit, "outbox_limit");
        let queue = Queue::new(options.queue_limit);
        let sub = Subscriber::new(options.actor_id, options.receiver, options.patterns);
        let signal = crate::mail::inbox::read_signal(&sub.inbox);
        let outbox = Outbox::new(outbox_limit, signal.clone());
        Core {
            make_job: options.make_job,
            on_rejected: options.on_rejected,
            active_limit,
            work_budget,
            timeout,
            queue,
            sub,
            signal,
            outbox,
            jobs: HashMap::new(),
            book: RequestBook::new(),
            phase: 0,
            busy: false,
            state: HostState::Running,
            rejected_count: 0,
            cancel_errors: 0,
            last_cancel_error: None,
            cancel_pending: HashSet::new(),
            abandoned: None,
            pending: None,
            pending_job: None,
            rest: Vec::new(),
            again: std::collections::HashSet::new(),
            attempts: std::collections::HashMap::new(),
        }
    }

    pub fn take_cancelled(&mut self) -> Vec<EnvelopeId> {
        self.book.take_cancelled()
    }

    pub fn cancel_jobs(&mut self, request_ids: &[EnvelopeId]) -> Result<(), BoxError> {
        if request_ids.is_empty() {
            return Ok(());
        }
        let body = crate::envelope::result::err_payload("cancelled", None)?;
        for request_id in request_ids {
            let job = self.jobs.get(request_id).cloned();
            let Some(job) = job else {
                self.cancel_pending.remove(request_id);
                continue;
            };
            let active = {
                let state = job.state.borrow();
                state.lifecycle == crate::job::job::Lifecycle::Active && !state.held
            };
            if !active {
                self.cancel_pending.remove(request_id);
                continue;
            }
            if !crate::job::ending::cancel_job(self, &job, &body)? {
                self.cancel_pending.insert(request_id.clone());
            }
        }
        Ok(())
    }

    pub fn take_pending_cancels(&mut self) -> HashSet<EnvelopeId> {
        std::mem::take(&mut self.cancel_pending)
    }

    pub fn cancel_queued(&mut self, request_ids: &[EnvelopeId]) {
        if request_ids.is_empty() {
            return;
        }
        self.queue
            .remove(|envelope| request_ids.contains(&envelope.header.id));
        for request_id in request_ids {
            remove(&self.sub.inbox, request_id);
        }
    }

    pub fn cancel_held(&mut self, request_ids: &[EnvelopeId]) {
        for job in self.jobs.values() {
            let root_id = job.root().header.id.clone();
            if !request_ids.contains(&root_id) {
                continue;
            }
            let state = job.state.borrow();
            if !state.held {
                continue;
            }
            if let Some(slot) = &state.reply_slot {
                let _ = slot.fail("cancelled", None);
            }
        }
    }

    pub fn begin_close(&mut self) {
        if self.state != HostState::Closed {
            self.state = HostState::Closing;
        }
        if self.abandoned.is_none() {
            self.abandoned = Some(Vec::new());
        }
    }

    pub fn abandoned_mut(&mut self) -> &mut Vec<Rc<Envelope>> {
        self.begin_close();
        self.abandoned.as_mut().expect("begin_close keeps the list")
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::sync::Arc;

    use crate::envelope::ids::{actor_id, envelope_id};
    use crate::envelope::{make_envelope, MakeEnvelope};
    use crate::job::job::{set_lifecycle, Job, JobHooks, Lifecycle};
    use crate::mail::inbox::Inbox;
    use crate::Payload;

    use super::{Core, CoreOptions};

    struct Hooks;

    impl JobHooks for Hooks {}

    #[test]
    fn cancel_retries_after_lease() {
        let mut core = Core::new(CoreOptions {
            actor_id: actor_id("test"),
            receiver: Inbox::new(8).receiver(),
            patterns: Vec::new(),
            request_timeout_ms: None,
            queue_limit: 8,
            active_limit: 8,
            work_budget: 8,
            outbox_limit: 8,
            make_job: Rc::new(|_root| Err("unused".into())),
            on_rejected: Rc::new(|_, _| Ok(())),
        });
        let root = Rc::new(
            make_envelope(MakeEnvelope {
                topic: "t".into(),
                payload: Arc::new(()) as Payload,
                from: actor_id("test"),
                cause: None,
                id: Some(envelope_id("req-1")),
                kind: "event".into(),
                reply_to: None,
                correlation_id: None,
            })
            .unwrap(),
        );
        let job = Job::new(root.clone(), Rc::new(Hooks));
        set_lifecycle(&mut job.state.borrow_mut(), Lifecycle::Active);
        let id = root.header.id.clone();
        core.jobs.insert(id.clone(), job);

        core.book.fail_next_cancel();
        core.cancel_jobs(std::slice::from_ref(&id)).unwrap();
        assert!(core.jobs.contains_key(&id));
        let pending: Vec<_> = core.take_pending_cancels().into_iter().collect();
        assert_eq!(pending, vec![id.clone()]);

        core.cancel_jobs(&pending).unwrap();
        assert!(core.jobs.is_empty());
        assert!(core.cancel_pending.is_empty());
        assert_eq!(core.cancel_errors, 0);
    }
}
