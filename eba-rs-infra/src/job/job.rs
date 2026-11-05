use crate::prelude::*;
use alloc::rc::Rc;
use core::cell::RefCell;

use crate::envelope::idgen::IdGen;
use crate::envelope::result::{Completion, ResultBody};
use crate::envelope::Envelope;
use crate::errors::BoxError;
use crate::job::effect::Effect;
use crate::job::job_lease::JobLease;

pub use crate::job::context::{seal_context, Commit, Context};

pub type SharedIdGen = Rc<RefCell<Box<dyn IdGen>>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    New,
    Active,
    Delivering,
    Finished,
}

impl core::fmt::Display for Lifecycle {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Lifecycle::New => "new",
            Lifecycle::Active => "active",
            Lifecycle::Delivering => "delivering",
            Lifecycle::Finished => "finished",
        })
    }
}

pub fn set_lifecycle(state: &mut State, lifecycle: Lifecycle) {
    let legal = matches!(
        (state.lifecycle, lifecycle),
        (Lifecycle::New, Lifecycle::Active)
            | (Lifecycle::Active, Lifecycle::Delivering)
            | (Lifecycle::Active, Lifecycle::Finished)
            | (Lifecycle::Delivering, Lifecycle::Finished)
            | (Lifecycle::Finished, Lifecycle::Finished)
    );
    if !legal {
        panic!(
            "illegal job transition: {} -> {}",
            state.lifecycle, lifecycle
        );
    }
    state.lifecycle = lifecycle;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Notice {
    Open,
    Rejected,
    Finished,
}

pub struct State {
    pub root: Rc<Envelope>,
    pub max_inflight: usize,
    pub lifecycle: Lifecycle,
    pub notice: Notice,
    pub replied: bool,
    pub held: bool,
    pub reply_slot: Option<JobLease>,
    pub reply_body: Option<ResultBody>,
    pub pending_control: Option<Control>,
}

pub trait JobHooks {
    fn begin_effects(&self, ctx: &Context) -> Result<Vec<Effect>, BoxError> {
        self.begin(ctx)?;
        Ok(Vec::new())
    }
    fn begin(&self, _ctx: &Context) -> Result<(), BoxError> {
        Ok(())
    }
    fn on_stage_effects(
        &self,
        ctx: &Context,
        stage: &str,
        completion: Rc<Completion>,
    ) -> Result<Vec<Effect>, BoxError> {
        self.on_stage_result(ctx, stage, completion)?;
        Ok(Vec::new())
    }
    fn on_stage_result(
        &self,
        _ctx: &Context,
        _stage: &str,
        _completion: Rc<Completion>,
    ) -> Result<(), BoxError> {
        Ok(())
    }
    fn on_resume(&self, _ctx: &Context) -> Result<Vec<Effect>, BoxError> {
        Ok(Vec::new())
    }

    fn on_finished_effects(&self, body: Option<&ResultBody>) -> Result<Vec<Effect>, BoxError> {
        self.on_finished(body)?;
        Ok(Vec::new())
    }

    fn on_finished(&self, _body: Option<&ResultBody>) -> Result<(), BoxError> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct Job {
    pub state: Rc<RefCell<State>>,
    pub hooks: Rc<dyn JobHooks>,
}

impl Job {
    pub fn new(root: Rc<Envelope>, hooks: Rc<dyn JobHooks>) -> Self {
        Job {
            state: Rc::new(RefCell::new(State {
                root,
                max_inflight: 100,
                lifecycle: Lifecycle::New,
                notice: Notice::Open,
                replied: false,
                held: false,
                reply_slot: None,
                reply_body: None,
                pending_control: None,
            })),
            hooks,
        }
    }

    pub fn root(&self) -> Rc<Envelope> {
        self.state.borrow().root.clone()
    }

    pub fn finished(&self) -> bool {
        self.state.borrow().lifecycle == Lifecycle::Finished
    }

    /// 返回 `Job` 内部共享的状态句柄；构造器是 `Job::new`。
    pub fn state_ref(&self) -> Rc<RefCell<State>> {
        Rc::clone(&self.state)
    }
}

#[derive(Clone)]
pub struct Intent {
    pub stage: String,
    pub request: Rc<Envelope>,
    pub timeout: Option<u64>,
}

#[derive(Clone)]
pub struct Control {
    pub body: Option<ResultBody>,
}
