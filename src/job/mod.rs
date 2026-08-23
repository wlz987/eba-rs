use crate::envelope::{ActorId, Envelope, EnvelopeId, Value};
use crate::jobhost::JobHost;
use crate::registry::{StartParams, State};
use crate::reply::reply;
use crate::{Error, Result};
use std::collections::{HashMap, VecDeque};
use std::sync::OnceLock;

pub const DEFAULT_MAX_INFLIGHT: usize = 100;

pub fn timeout_body() -> &'static Value {
    static BODY: OnceLock<Value> = OnceLock::new();
    BODY.get_or_init(|| crate::result::err_payload("request_timeout", &[]).expect("timeout body"))
}

#[derive(Debug, Clone)]
pub(crate) struct Inflight {
    pub stage: String,
    pub deadline_ms: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct Deferred {
    pub stage: String,
    pub request_prefix: String,
    pub result_prefix: String,
    pub payload: Value,
    pub request_id: EnvelopeId,
}

#[derive(Debug, Clone)]
pub struct RequestSpec {
    pub stage: String,
    pub request_prefix: String,
    pub result_prefix: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Default)]
pub struct FinishInfo {
    pub ok: bool,
    pub error: Option<String>,
    pub answer: Option<Value>,
}

impl FinishInfo {
    pub fn ok_answer(answer: Value) -> FinishInfo {
        FinishInfo {
            ok: true,
            error: None,
            answer: Some(answer),
        }
    }

    pub fn err_msg(msg: impl Into<String>) -> FinishInfo {
        FinishInfo {
            ok: false,
            error: Some(msg.into()),
            answer: None,
        }
    }
}

type BeginHook = Box<dyn FnMut(&mut JobCtx<'_>) -> Result<()>>;
type StageHook = Box<dyn FnMut(&mut JobCtx<'_>, &str, &Value) -> Result<()>>;
type FinishedHook = Box<dyn FnMut(&mut JobCtx<'_>, &FinishInfo) -> Result<()>>;

#[derive(Default)]
pub struct Job {
    pub root: Envelope,
    pub(crate) finished: bool,
    pub(crate) inflight: HashMap<EnvelopeId, Inflight>,
    pub(crate) deferred: VecDeque<Deferred>,
    pub(crate) max_inflight: usize,
    pub on_begin: Option<BeginHook>,
    pub on_stage_result: Option<StageHook>,
    pub on_finished: Option<FinishedHook>,
}

impl Job {
    pub fn new(root: Envelope) -> Job {
        Job {
            root,
            max_inflight: DEFAULT_MAX_INFLIGHT,
            ..Job::default()
        }
    }

    pub fn set_max_inflight(&mut self, n: usize) {
        assert!(n >= 1, "job: max_inflight must be >= 1");
        self.max_inflight = n;
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    pub(crate) fn ensure_requestable(&self) -> Result<()> {
        if self.finished {
            return Err(Error::State("job not requestable".into()));
        }
        Ok(())
    }
}

pub struct JobCtx<'a> {
    pub job: &'a mut Job,
    pub(crate) host: &'a mut JobHost,
}

impl<'a> JobCtx<'a> {
    pub fn begin(&mut self) -> Result<()> {
        if let Some(mut hook) = self.job.on_begin.take() {
            let res = hook(self);
            self.job.on_begin = Some(hook);
            res?;
        }
        Ok(())
    }

    pub fn request(&mut self, spec: RequestSpec) -> Result<EnvelopeId> {
        if self.host.cont_depth > 0 {
            self.defer_request(spec)
        } else {
            self.issue(spec, None)
        }
    }

    pub fn reply(&mut self, result_prefix: &str, body: &Value) -> Result<()> {
        let loan = self.host.require_loan()?;
        reply(
            &loan.bus,
            &self.job.root,
            body,
            result_prefix,
            self.host.actor_id.clone(),
            &loan.gen,
        )
    }

    pub fn finish(&mut self, info: FinishInfo) -> Result<()> {
        if self.job.finished {
            return Ok(());
        }
        self.job.finished = true;
        self.job.deferred.clear();
        self.drop_inflight()?;
        let mut hook = match self.job.on_finished.take() {
            Some(hook) => hook,
            None => return Ok(()),
        };
        let res = hook(self, &info);
        self.job.on_finished = Some(hook);
        res
    }

    pub(crate) fn deliver_child_result(&mut self, env: &Envelope) -> Result<()> {
        let Some(child_id) = crate::result::body::result_request_id(env) else {
            return Ok(());
        };
        let Some(body) = crate::result::body::result_body(env) else {
            return Ok(());
        };
        let entry = self.job.inflight.remove(&child_id);
        let loan_bus = self.host.require_loan()?.bus.clone();
        self.host
            .registry
            .finish_safe(&loan_bus, &self.host.inbox, &child_id);
        let Some(entry) = entry else {
            return Ok(());
        };
        if self.job.finished {
            return Ok(());
        }
        self.emit(&entry.stage, body)
    }

    pub(crate) fn expire_due(&mut self, now_ms: i64) -> Result<()> {
        let due: Vec<EnvelopeId> = self
            .job
            .inflight
            .iter()
            .filter(|(_, ent)| ent.deadline_ms != 0 && now_ms >= ent.deadline_ms)
            .map(|(id, _)| id.clone())
            .collect();
        for id in due {
            let Some(entry) = self.job.inflight.remove(&id) else {
                continue;
            };
            let loan_bus = self.host.require_loan()?.bus.clone();
            if self.host.registry.state(&id) == Some(State::Pending) {
                self.host.registry.timeout(&id);
            }
            self.host
                .registry
                .finish_safe(&loan_bus, &self.host.inbox, &id);
            if !self.job.finished {
                self.emit(&entry.stage, timeout_body())?;
            }
        }
        Ok(())
    }

    fn defer_request(&mut self, spec: RequestSpec) -> Result<EnvelopeId> {
        self.job.ensure_requestable()?;
        if self.job.inflight.len() + self.job.deferred.len() >= self.job.max_inflight {
            return Err(Error::MaxInflight);
        }
        let eid = self
            .host
            .require_loan()?
            .gen
            .borrow_mut()
            .next_envelope_id();
        self.job.deferred.push_back(Deferred {
            stage: spec.stage,
            request_prefix: spec.request_prefix,
            result_prefix: spec.result_prefix,
            payload: spec.payload,
            request_id: eid.clone(),
        });
        Ok(eid)
    }

    fn issue(&mut self, spec: RequestSpec, request_id: Option<EnvelopeId>) -> Result<EnvelopeId> {
        self.job.ensure_requestable()?;
        if self.job.inflight.len() >= self.job.max_inflight {
            return Err(Error::MaxInflight);
        }
        let child = {
            let loan = self.host.require_loan()?;
            let bus = loan.bus.clone();
            let gen = loan.gen.clone();
            self.host.registry.start_request(
                &bus,
                &self.host.inbox,
                &gen,
                StartParams {
                    request_prefix: spec.request_prefix,
                    result_prefix: spec.result_prefix.clone(),
                    payload: spec.payload,
                    from: ActorId(self.host.actor_id.0.clone()),
                    cause: self.job.root.header.cause.clone(),
                    request_id,
                },
            )?
        };
        let timeout = self.host.request_timeout_ms;
        let deadline = if timeout == 0 {
            0
        } else {
            let loan = self.host.require_loan()?;
            loan.clock.now_ms() + timeout
        };
        let child_id = child.header.id.clone();
        self.job.inflight.insert(
            child_id.clone(),
            Inflight {
                stage: spec.stage,
                deadline_ms: deadline,
            },
        );
        Ok(child_id)
    }

    fn flush_deferred(&mut self) -> Result<()> {
        while !self.job.deferred.is_empty() && !self.job.finished {
            let nxt = self.job.deferred.pop_front().expect("deferred nonempty");
            self.issue(
                RequestSpec {
                    stage: nxt.stage,
                    request_prefix: nxt.request_prefix,
                    result_prefix: nxt.result_prefix,
                    payload: nxt.payload,
                },
                Some(nxt.request_id),
            )?;
        }
        Ok(())
    }

    fn emit(&mut self, stage: &str, body: &Value) -> Result<()> {
        self.host.cont_depth += 1;
        let res = self.emit_inner(stage, body);
        self.host.cont_depth -= 1;
        res
    }

    fn emit_inner(&mut self, stage: &str, body: &Value) -> Result<()> {
        let mut hook = match self.job.on_stage_result.take() {
            Some(hook) => hook,
            None => return self.flush_deferred(),
        };
        let res = hook(self, stage, body);
        self.job.on_stage_result = Some(hook);
        res?;
        self.flush_deferred()
    }

    fn drop_inflight(&mut self) -> Result<()> {
        let ids: Vec<EnvelopeId> = self.job.inflight.keys().cloned().collect();
        if ids.is_empty() {
            return Ok(());
        }
        let loan_bus = self.host.require_loan()?.bus.clone();
        for id in ids {
            self.job.inflight.remove(&id);
            if self.host.registry.state(&id) == Some(State::Pending) {
                self.host.registry.timeout(&id);
            }
            self.host
                .registry
                .finish_safe(&loan_bus, &self.host.inbox, &id);
        }
        Ok(())
    }
}
