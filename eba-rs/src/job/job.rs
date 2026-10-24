use std::cell::RefCell;
use std::rc::Rc;

use crate::envelope::idgen::IdGen;
use crate::envelope::result::{Completion, ResultBody};
use crate::envelope::Envelope;
use crate::errors::BoxError;
use crate::job::reply::Reply;

pub use crate::job::context::{seal_context, Commit, Context};

pub type SharedIdGen = Rc<RefCell<Box<dyn IdGen>>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    New,
    Active,
    Delivering,
    Finished,
}

impl std::fmt::Display for Lifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    pub reply_slot: Option<Reply>,
    pub reply_body: Option<ResultBody>,
    pub pending_control: Option<Control>,
}

pub trait JobHooks {
    fn begin(&self, _ctx: &Context) -> Result<(), BoxError> {
        Ok(())
    }
    fn on_stage_result(
        &self,
        _ctx: &Context,
        _stage: &str,
        _completion: Rc<Completion>,
    ) -> Result<(), BoxError> {
        Ok(())
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

    /// 账来；这边账是 `Job` 的字段，构造器就是 `Job::new`，那一问记在 `tests/boundaries.rs` 的账上。
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
