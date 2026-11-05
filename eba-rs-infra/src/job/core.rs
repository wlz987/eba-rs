use crate::prelude::*;
use alloc::collections::{BTreeMap, BTreeSet};
use alloc::rc::Rc;
use core::cell::RefCell;

use crate::envelope::ids::ActorId;
use crate::envelope::{Envelope, EnvelopeId};
use crate::errors::{BoxError, BusError};
use crate::job::effect::EffectHost;
use crate::job::job::Job;
use crate::job::lease_registry::LeaseRegistry;
use crate::job::mailbox::Mailbox;
use crate::mail::inbox::remove;
use crate::mail::inbox::Receiver;
use crate::mail::queue::Queue;
use crate::mail::subscriber::Subscriber;
use crate::notify::Signal;
use crate::pattern::parse_pattern;
use crate::record::{MemorySink, RecordLog, RecordSink};
use crate::time::clock::Clock;
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

pub type ResumeHook = Box<dyn FnOnce(&mut Core)>;

pub struct Core {
    pub make_job: MakeJob,
    pub on_rejected: OnRejected,
    pub active_limit: usize,
    pub work_budget: usize,
    pub timeout: Option<u64>,
    pub queue: Queue<Rc<Envelope>>,
    pub sub: Subscriber,
    pub signal: Rc<Signal>,
    pub mailbox: Mailbox,
    pub records: Rc<RefCell<RecordLog>>,
    pub timers: Rc<RefCell<Vec<u64>>>,
    pub host: Option<Rc<dyn EffectHost>>,
    pub clock: Option<Rc<dyn Clock>>,
    pub deferred_barriers: Vec<(u64, ResumeHook)>,
    pub deferred_timers: Vec<(u64, ResumeHook)>,
    pub jobs: BTreeMap<EnvelopeId, Job>,
    pub book: LeaseRegistry,
    pub phase: usize,
    pub busy: bool,
    pub state: HostState,
    pub rejected_count: usize,
    pub cancel_pending: BTreeSet<EnvelopeId>,
    pub abandoned: Option<Vec<Rc<Envelope>>>,
    pub pending: Option<Rc<Envelope>>,
    pub pending_job: Option<Job>,
    pub rest: Vec<(Rc<Envelope>, Option<Job>)>,
    pub again: alloc::collections::BTreeSet<EnvelopeId>,
    pub attempts: alloc::collections::BTreeMap<EnvelopeId, usize>,
}

pub struct CoreOptions {
    pub actor_id: ActorId,
    pub receiver: Receiver,
    pub patterns: Vec<String>,
    pub request_timeout_ms: Option<u64>,
    pub queue_limit: usize,
    pub active_limit: usize,
    pub work_budget: usize,
    pub mailbox_limit: usize,
    pub make_job: MakeJob,
    pub on_rejected: OnRejected,
    /// Durable log for `record`/`barrier` effects. `None` selects `MemorySink`.
    pub record_sink: Option<Box<dyn RecordSink>>,
}

impl CoreOptions {
    pub const DEFAULT_QUEUE_LIMIT: usize = 512;
    pub const DEFAULT_ACTIVE_LIMIT: usize = 256;
    pub const DEFAULT_WORK_BUDGET: usize = 64;
    pub const DEFAULT_MAILBOX_LIMIT: usize = 512;
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
        if options.mailbox_limit < 1 {
            return Err(
                Box::new(BusError("mailbox_limit must be a positive integer".into())) as BoxError,
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
        let mailbox_limit = positive_int(options.mailbox_limit, "mailbox_limit");
        let queue = Queue::new(options.queue_limit);
        let sub = Subscriber::new(options.actor_id, options.receiver, options.patterns);
        let signal = crate::mail::inbox::read_signal(&sub.inbox);
        let mailbox = Mailbox::new(mailbox_limit, signal.clone());
        let records = Rc::new(RefCell::new(match options.record_sink {
            Some(sink) => RecordLog::new(sink, 64, 1),
            None => RecordLog::new(MemorySink::new(), 64, 1),
        }));
        Core {
            make_job: options.make_job,
            on_rejected: options.on_rejected,
            active_limit,
            work_budget,
            timeout,
            queue,
            sub,
            signal,
            mailbox,
            records,
            timers: Rc::new(RefCell::new(Vec::new())),
            host: None,
            clock: None,
            deferred_barriers: Vec::new(),
            deferred_timers: Vec::new(),
            jobs: BTreeMap::new(),
            book: LeaseRegistry::new(),
            phase: 0,
            busy: false,
            state: HostState::Running,
            rejected_count: 0,
            cancel_pending: BTreeSet::new(),
            abandoned: None,
            pending: None,
            pending_job: None,
            rest: Vec::new(),
            again: alloc::collections::BTreeSet::new(),
            attempts: alloc::collections::BTreeMap::new(),
        }
    }

    pub fn drain_records(&self, limit: usize) -> Result<usize, BoxError> {
        self.records.borrow_mut().drain(limit)
    }

    pub fn flush_records(&mut self, seq: u64) -> Result<(), BoxError> {
        self.records.borrow_mut().flush(seq)?;
        let ready: Vec<_> = {
            let mut kept = Vec::new();
            let mut ready = Vec::new();
            for (at, resume) in self.deferred_barriers.drain(..) {
                if at <= seq {
                    ready.push(resume);
                } else {
                    kept.push((at, resume));
                }
            }
            self.deferred_barriers = kept;
            ready
        };
        for resume in ready {
            resume(self);
        }
        Ok(())
    }

    pub fn checkpoint_records(&self, seq: u64) -> Result<u64, BoxError> {
        self.records.borrow_mut().checkpoint(seq)
    }

    /// Per-turn record settlement: drain the pending ring into the sink, then
    /// advance the durable watermark to the last appended seq and wake any
    /// barrier whose target has been reached. Returns true when the ring was
    /// drained and/or the durable watermark advanced.
    pub fn settle_records(&mut self) -> Result<bool, BoxError> {
        if self.records.borrow().pending() == 0 {
            return Ok(false);
        }
        self.drain_records(usize::MAX)?;
        let target = self.records.borrow().next_seq() - 1;
        let durable = self.records.borrow().durable();
        if target > durable {
            self.flush_records(target)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn arm_timer(&self, deadline_ms: u64) {
        self.timers.borrow_mut().push(deadline_ms);
    }

    pub fn next_timer_ms(&self) -> Option<u64> {
        self.timers.borrow().iter().copied().min()
    }

    pub fn fire_due_timers(&mut self, now_ms: u64) -> usize {
        let mut timers = self.timers.borrow_mut();
        let before = timers.len();
        timers.retain(|deadline| *deadline > now_ms);
        let due = before - timers.len();
        if due > 0 {
            self.signal.notify(crate::notify::signal::TIMER);
        }
        drop(timers);
        let ready: Vec<_> = {
            let mut kept = Vec::new();
            let mut ready = Vec::new();
            for (deadline, resume) in self.deferred_timers.drain(..) {
                if deadline <= now_ms {
                    ready.push(resume);
                } else {
                    kept.push((deadline, resume));
                }
            }
            self.deferred_timers = kept;
            ready
        };
        for resume in ready {
            resume(self);
        }
        due
    }

    pub fn defer_barrier(&mut self, seq: u64, resume: ResumeHook) {
        self.deferred_barriers.push((seq, resume));
    }

    pub fn defer_timer(&mut self, deadline_ms: u64, resume: ResumeHook) {
        self.deferred_timers.push((deadline_ms, resume));
    }

    pub fn charge_leases(&mut self, delta_ms: u64) -> Result<(), BoxError> {
        let mut changed = false;
        for job in self.jobs.values() {
            let slot = job.state.borrow().reply_slot.clone();
            if let Some(slot) = slot {
                if slot.is_closed() {
                    continue;
                }
                if !slot.consume(delta_ms) {
                    slot.revoke("budget")?;
                    changed = true;
                }
            }
        }
        if changed {
            self.signal.notify(crate::notify::signal::WORK);
        }
        Ok(())
    }

    pub fn expire_leases(&mut self, now_ms: u64) -> Result<bool, BoxError> {
        let mut expired = false;
        for job in self.jobs.values() {
            let slot = job.state.borrow().reply_slot.clone();
            if let Some(slot) = slot {
                if slot.is_expired(now_ms) {
                    slot.revoke("deadline")?;
                    expired = true;
                }
            }
        }
        if expired {
            self.signal.notify(crate::notify::signal::WORK);
        }
        Ok(expired)
    }

    pub fn take_cancelled(&mut self) -> Vec<EnvelopeId> {
        self.book.take_cancelled()
    }

    pub fn revoke(&mut self, request_ids: &[EnvelopeId]) -> Result<(), BoxError> {
        self.revoke_queued(request_ids);
        self.revoke_held(request_ids);
        self.revoke_active(request_ids)
    }

    fn revoke_active(&mut self, request_ids: &[EnvelopeId]) -> Result<(), BoxError> {
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

    pub fn take_pending_cancels(&mut self) -> BTreeSet<EnvelopeId> {
        core::mem::take(&mut self.cancel_pending)
    }

    fn revoke_queued(&mut self, request_ids: &[EnvelopeId]) {
        if request_ids.is_empty() {
            return;
        }
        self.queue
            .remove(|envelope| request_ids.contains(&envelope.header.id));
        for request_id in request_ids {
            remove(&self.sub.inbox, request_id);
        }
    }

    fn revoke_held(&mut self, request_ids: &[EnvelopeId]) {
        for job in self.jobs.values() {
            let root_id = job.root().header.id.clone();
            if !request_ids.contains(&root_id) {
                continue;
            }
            let slot = {
                let state = job.state.borrow();
                if !state.held {
                    continue;
                }
                state.reply_slot.clone()
            };
            if let Some(slot) = slot {
                let _ = slot.revoke("cancelled");
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
