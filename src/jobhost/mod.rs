use crate::bus::BusHandle;
use crate::envelope::{ActorId, Envelope, EnvelopeId};
use crate::idgen::IdGenHandle;
use crate::inbox::InboxHandle;
use crate::job::Job;
use crate::jobhost::dispatch::{dispatch, flush_queue, watchdog};
use crate::jobhost::slots::SlotBook;
use crate::pattern::{parse_pattern, Pattern};
use crate::registry::Registry;
use crate::subscriber::Subscriber;
use queue::EnvelopeQueue;
pub use queue::DEFAULT_QUEUE_LIMIT;
use std::rc::Rc;

pub struct HostParams {
    pub actor_id: ActorId,
    pub inbox: InboxHandle,
    pub patterns: Vec<String>,
    pub accept: Option<Vec<String>>,
    pub request_timeout_ms: i64,
    pub queue_limit: usize,
    pub make_job: Box<dyn Fn(&Envelope) -> Job>,
}

pub(crate) mod dispatch;
pub(crate) mod queue;
pub(crate) mod slots;

#[derive(Clone)]
pub(crate) struct Loan {
    pub bus: BusHandle,
    pub clock: crate::clock::ClockHandle,
    pub gen: IdGenHandle,
}

pub struct JobHost {
    pub(crate) actor_id: ActorId,
    pub(crate) inbox: InboxHandle,
    pub(crate) request_timeout_ms: i64,
    pub(crate) registry: Registry,
    subscriber: Subscriber,
    pub(crate) accept: Vec<Pattern>,
    pub(crate) slots: SlotBook,
    pub(crate) queue: EnvelopeQueue,
    pub(crate) cont_depth: usize,
    pub(crate) loan: Option<Loan>,
    pub(crate) shutting_down: bool,
    busy: bool,
    pub(crate) make_job: Box<dyn Fn(&Envelope) -> Job>,
}

impl JobHost {
    pub fn new(params: HostParams) -> JobHost {
        let limit = if params.queue_limit == 0 {
            DEFAULT_QUEUE_LIMIT
        } else {
            params.queue_limit
        };
        let src = params.accept.as_ref().unwrap_or(&params.patterns);
        let mut accept = Vec::with_capacity(src.len());
        for text in src {
            match parse_pattern(text) {
                Ok(p) => accept.push(p),
                Err(err) => panic!("jobhost: {err}"),
            }
        }
        let patterns_ref: Vec<&str> = params.patterns.iter().map(|s| s.as_str()).collect();
        let subscriber = Subscriber::new(params.actor_id.clone(), &params.inbox, &patterns_ref);
        JobHost {
            actor_id: params.actor_id,
            inbox: params.inbox,
            request_timeout_ms: params.request_timeout_ms,
            registry: Registry::new(),
            subscriber,
            accept,
            slots: SlotBook::new(),
            queue: EnvelopeQueue::new(limit),
            cont_depth: 0,
            loan: None,
            shutting_down: false,
            busy: false,
            make_job: params.make_job,
        }
    }

    pub fn actor_id(&self) -> &ActorId {
        &self.actor_id
    }

    pub fn inbox(&self) -> &InboxHandle {
        &self.inbox
    }

    pub fn start(&self, bus: &crate::bus::Bus) -> crate::Result<()> {
        self.subscriber.start(bus)
    }

    pub fn job(&self, cause: &EnvelopeId) -> Option<&Job> {
        self.slots.active_job(cause)
    }

    pub fn shutdown(&mut self) {
        self.shutting_down = true;
    }

    pub fn handle(
        &mut self,
        bus: BusHandle,
        env: &Envelope,
        clock: crate::clock::ClockHandle,
        gen: IdGenHandle,
    ) -> crate::Result<()> {
        if self.busy {
            return Err(crate::Error::State("reentrant handle".into()));
        }
        self.busy = true;
        self.set_loan(bus, clock, gen);
        let res = (|| {
            let empty = self.inbox.is_empty();
            self.run_dispatch(env)?;
            if empty {
                self.drain_inbox()?;
            }
            Ok(())
        })();
        self.clear_loan();
        self.busy = false;
        res
    }

    pub fn poll(
        &mut self,
        bus: BusHandle,
        clock: crate::clock::ClockHandle,
        gen: IdGenHandle,
    ) -> crate::Result<bool> {
        match self.inbox.try_recv() {
            None => {
                if self.busy {
                    return Err(crate::Error::State("reentrant handle".into()));
                }
                self.busy = true;
                self.set_loan(Rc::clone(&bus), Rc::clone(&clock), Rc::clone(&gen));
                let res = watchdog(self);
                self.clear_loan();
                self.busy = false;
                res.map(|_| false)
            }
            Some(env) => self.handle(bus, &env, clock, gen).map(|_| true),
        }
    }

    fn run_dispatch(&mut self, env: &Envelope) -> crate::Result<()> {
        dispatch(self, env)?;
        watchdog(self)?;
        flush_queue(self)
    }

    fn drain_inbox(&mut self) -> crate::Result<()> {
        loop {
            let Some(env) = self.inbox.try_recv() else {
                return Ok(());
            };
            self.run_dispatch(&env)?;
        }
    }

    pub(crate) fn set_loan(
        &mut self,
        bus: BusHandle,
        clock: crate::clock::ClockHandle,
        gen: IdGenHandle,
    ) {
        self.registry.bind_id_gen(&gen);
        self.loan = Some(Loan { bus, clock, gen });
    }

    pub(crate) fn clear_loan(&mut self) {
        self.loan = None;
    }

    pub(crate) fn require_loan(&self) -> crate::Result<&Loan> {
        self.loan.as_ref().ok_or_else(|| {
            crate::Error::State("Bus/Clock/IdGen borrowed only during handle/poll".into())
        })
    }
}

#[cfg(test)]
mod orphan_result {
    use super::*;
    use crate::clock::ManualClock;
    use crate::envelope::{make_envelope, MakeOptions, Value};
    use crate::idgen::SeqIdGen;
    use crate::inbox::Inbox;
    use crate::job::{FinishInfo, Job};
    use crate::registry::{result_topic_of, StartParams, State};
    use crate::result::ok_payload;
    use crate::Bus;
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::rc::Rc;

    #[test]
    fn fresh_result_without_job_finishes_registry() {
        let bus = Bus::new();
        let mut host = JobHost::new(HostParams {
            actor_id: ActorId("h".into()),
            inbox: Inbox::new(8),
            patterns: vec!["job".into()],
            accept: None,
            request_timeout_ms: 0,
            queue_limit: 0,
            make_job: Box::new(|root| {
                let mut job = Job::new(root.clone());
                job.on_begin = Some(Box::new(|ctx| {
                    ctx.finish(FinishInfo::ok_answer(Value::Null))
                }));
                job
            }),
        });
        host.start(&bus).unwrap();
        let gen: IdGenHandle = Rc::new(RefCell::new(SeqIdGen::new(1)));
        let cause = gen.borrow_mut().next_envelope_id();
        let req = host
            .registry
            .start_request(
                &bus,
                &host.inbox,
                &gen,
                StartParams {
                    request_prefix: "acl".into(),
                    result_prefix: "acl.result".into(),
                    payload: Value::Null,
                    from: ActorId("h".into()),
                    cause: cause.clone(),
                    request_id: None,
                },
            )
            .unwrap();
        let rid = req.header.id.clone();
        assert_eq!(host.registry.state(&rid), Some(State::Pending));
        let topic = result_topic_of(&rid, "acl.result", &gen).unwrap();
        let mut payload = BTreeMap::new();
        payload.insert("request_id".into(), Value::Str(rid.0.clone()));
        payload.insert("result".into(), ok_payload(Value::Str("allow".into())));
        let echo = make_envelope(
            &topic,
            Value::Object(payload),
            ActorId("leaf".into()),
            MakeOptions {
                cause: Some(cause),
                id_gen: Some(gen.clone()),
                ..Default::default()
            },
        )
        .unwrap();
        let clk = Rc::new(ManualClock::new(0));
        host.handle(bus, &echo, clk, gen).unwrap();
        assert_eq!(host.registry.state(&rid), None);
    }
}
