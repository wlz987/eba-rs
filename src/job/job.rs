//! Job / Context / JobHost / Call(Wait)。
use crate::envelope::{
    make_envelope, Address, Completion, Envelope, EnvelopeId, IdGen, MakeEnvelope,
};
use crate::errors::{EbaError, Result};
use crate::job::effect::{Effect, Intent};
use crate::job::lease::{Lease, LeaseOptions};
use crate::prelude::*;
use crate::value::{err_payload, has_result_shape, ok_payload, Value};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lifecycle {
    New,
    Active,
    Delivering,
    Finished,
}
pub struct JobState {
    pub root: Envelope,
    pub max_inflight: usize,
    pub lifecycle: Lifecycle,
    pub held: bool,
    pub replied: bool,
    pub reply_body: Option<Value>,
    /// onFinished 至多调用一次，
    /// 防止 `Finish` effect 落地时与错误/撤销路径重复触发清理回调。
    pub notified: bool,
}
impl JobState {
    pub fn new(root: Envelope, max_inflight: usize) -> Self {
        Self {
            root,
            max_inflight,
            lifecycle: Lifecycle::New,
            held: false,
            replied: false,
            reply_body: None,
            notified: false,
        }
    }
    /// 强制合法迁移 New→Active→Delivering/Finished。
    pub fn transition(&mut self, to: Lifecycle) {
        let from = self.lifecycle;
        let legal = match from {
            Lifecycle::New => to == Lifecycle::Active,
            Lifecycle::Active => to == Lifecycle::Delivering || to == Lifecycle::Finished,
            Lifecycle::Delivering => to == Lifecycle::Finished,
            Lifecycle::Finished => to == Lifecycle::Finished,
        };
        assert!(legal, "illegal job transition: {:?} -> {:?}", from, to);
        self.lifecycle = to;
    }
    pub fn finished(&self) -> bool {
        self.lifecycle == Lifecycle::Finished
    }
}
/// hook 失败时落地 job_failed 结算，而非 panic 传播。
/// RS 保留原有的 infallible 方法作为默认实现；可失败的 hook 覆盖 try_* 方法。
pub trait JobHooks {
    fn try_begin(&self, ctx: &mut Context) -> core::result::Result<Vec<Effect>, String> {
        Ok(self.begin(ctx))
    }
    fn try_on_stage_result(
        &self,
        ctx: &mut Context,
        stage: &str,
        completion: Completion,
    ) -> core::result::Result<Vec<Effect>, String> {
        Ok(self.on_stage_result(ctx, stage, completion))
    }
    fn try_on_finished(&self, body: Option<Value>) -> core::result::Result<Vec<Effect>, String> {
        Ok(self.on_finished(body))
    }
    fn try_on_resume(&self, ctx: &mut Context) -> core::result::Result<Vec<Effect>, String> {
        Ok(self.on_resume(ctx))
    }
    /// 关闭排空在途 job 时回调，
    /// reason 为 host_closing / host_draining。
    fn on_rejected(&self, _root: &Envelope, _reason: &str) -> Vec<Effect> {
        Vec::new()
    }
    fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
        let _ = ctx;
        Vec::new()
    }
    fn on_stage_result(
        &self,
        ctx: &mut Context,
        stage: &str,
        completion: Completion,
    ) -> Vec<Effect> {
        let _ = (ctx, stage, completion);
        Vec::new()
    }
    fn on_finished(&self, body: Option<Value>) -> Vec<Effect> {
        let _ = body;
        Vec::new()
    }
    fn on_resume(&self, ctx: &mut Context) -> Vec<Effect> {
        let _ = ctx;
        Vec::new()
    }
}
pub struct Job {
    pub state: JobState,
    pub hooks: Box<dyn JobHooks>,
}
impl Job {
    pub fn new(root: Envelope, hooks: Box<dyn JobHooks>, max_inflight: usize) -> Self {
        Self {
            state: JobState::new(root, max_inflight),
            hooks,
        }
    }
    pub fn root(&self) -> &Envelope {
        &self.state.root
    }
    pub fn finished(&self) -> bool {
        self.state.finished()
    }
}
pub type JobFactory = dyn Fn(&Envelope) -> Box<dyn JobHooks>;
/// Context：纯 builder，只造 Effect 意图，落地由 apply_effects 完成。
pub struct Context<'a> {
    pub job: &'a mut JobState,
    pub effects: Vec<Effect>,
    actor_id: String,
    reply_to: String,
    id_gen: &'a dyn IdGen,
    ask_count: usize,
    /// builder 期的可捕获校验错误（为 throw）；run_hook 在 hook 返回后取走。
    pub error: RefCell<Option<String>>,
}
impl<'a> Context<'a> {
    pub fn new(
        job: &'a mut JobState,
        actor_id: impl Into<String>,
        reply_to: impl Into<String>,
        id_gen: &'a dyn IdGen,
    ) -> Self {
        Self {
            job,
            effects: Vec::new(),
            actor_id: actor_id.into(),
            reply_to: reply_to.into(),
            id_gen,
            ask_count: 0,
            error: RefCell::new(None),
        }
    }
    pub fn root(&self) -> &Envelope {
        &self.job.root
    }
    pub fn finished(&self) -> bool {
        // 仅 finish / control / lifecycle 算 finished，
        // ctx.close() 是宿主关闭 effect，不影响此判定。
        self.effects
            .iter()
            .any(|e| matches!(e, Effect::Finish { .. }))
            || self.job.lifecycle == Lifecycle::Finished
    }
    pub fn push_effect(&mut self, effect: Effect) {
        self.effects.push(effect);
    }
    pub fn take_effects(&mut self) -> Vec<Effect> {
        core::mem::take(&mut self.effects)
    }
    /// 合并 hook 返回值与 ctx.push 的效果，兼容两种写法。
    pub fn collect(&mut self, returned: Vec<Effect>) -> Vec<Effect> {
        let mut out = core::mem::take(&mut self.effects);
        out.extend(returned);
        out
    }
    /// 造 ask Effect（纯 builder）。
    pub fn ask(
        &mut self,
        stage: impl Into<String>,
        topic: impl Into<String>,
        payload: Value,
        timeout_ms: Option<u64>,
    ) -> Result<Effect> {
        self.ask_from(self.actor_id.clone(), stage, topic, payload, timeout_ms)
    }
    /// 带显式 from 的 ask（Wait 用调用方身份发业务请求）。
    pub fn ask_from(
        &mut self,
        from: impl Into<String>,
        stage: impl Into<String>,
        topic: impl Into<String>,
        payload: Value,
        timeout_ms: Option<u64>,
    ) -> Result<Effect> {
        let request = make_envelope(MakeEnvelope {
            topic: topic.into(),
            payload,
            from: from.into(),
            id_gen: self.id_gen,
            address: Address::Request {
                reply_to: self.reply_to.clone(),
            },
            // 74：出站 ask 的 cause 继承 root 的原始 cause。
            cause: self.job.root.header.cause.clone(),
            sv: 1,
        })?;
        self.ask_count += 1;
        Ok(Effect::Ask {
            intent: Intent {
                stage: stage.into(),
                request,
                timeout_ms,
                reply_to: self.reply_to.clone(),
            },
        })
    }
    /// 请求并登记（ask + push），返回 request id。
    pub fn request(
        &mut self,
        stage: impl Into<String>,
        topic: impl Into<String>,
        payload: Value,
        timeout_ms: Option<u64>,
    ) -> Result<EnvelopeId> {
        let effect = self.ask(stage, topic, payload, timeout_ms)?;
        let id = match &effect {
            Effect::Ask { intent } => intent.request.header.id.clone(),
            _ => unreachable!(),
        };
        self.push_effect(effect);
        Ok(id)
    }
    pub fn reply_effect(&self, body: Value) -> Effect {
        // 抛 TypeError，可捕获；panic=abort 下记入错误槽。
        if !has_result_shape(&body) {
            *self.error.borrow_mut() = Some("reply body must be a result shape".into());
        }
        Effect::Reply { body }
    }
    pub fn finish_effect(&self, body: Value) -> Effect {
        if !has_result_shape(&body) {
            *self.error.borrow_mut() = Some("finish body must be a result shape".into());
        }
        Effect::Finish { body }
    }
    pub fn reply(&mut self, body: Value) {
        let e = self.reply_effect(body);
        self.push_effect(e);
    }
    pub fn finish(&mut self, body: Value) {
        let e = self.finish_effect(body);
        self.push_effect(e);
    }
    pub fn tell(&self, topic: impl Into<String>, payload: Value) -> Effect {
        Effect::Tell {
            topic: topic.into(),
            payload,
        }
    }
    pub fn record(&self, topic: impl Into<String>, payload: Value) -> Effect {
        Effect::Record {
            topic: topic.into(),
            payload,
        }
    }
    pub fn barrier(&self, up_to: Option<u64>) -> Effect {
        Effect::Barrier { up_to }
    }
    pub fn timer(&self, deadline_ms: u64) -> Effect {
        Effect::Timer { deadline_ms }
    }
    pub fn close(&self, reason: Option<String>) -> Effect {
        Effect::Close { reason }
    }
    pub fn hold(&mut self, options: LeaseOptions) -> Result<Lease> {
        if !self.effects.is_empty() {
            return Err(EbaError::Other("cannot hold after other effects".into()));
        }
        let lease = Lease::new(options);
        lease.bind(self.job.root.header.id.clone());
        self.push_effect(Effect::Hold {
            lease: lease.clone(),
        });
        Ok(lease)
    }
    pub fn actor_id(&self) -> &str {
        &self.actor_id
    }
    pub fn reply_to(&self) -> &str {
        &self.reply_to
    }
}
/// 每个 host 的可调参数。
/// 默认值与 Core::new 的内建上限一致。
#[derive(Clone)]
pub struct HostOptions {
    pub request_timeout_ms: Option<u64>,
    pub queue_limit: usize,
    pub active_limit: usize,
    pub work_budget: usize,
    pub mailbox_limit: usize,
}
impl Default for HostOptions {
    fn default() -> Self {
        Self {
            request_timeout_ms: None,
            queue_limit: 512,
            active_limit: 256,
            work_budget: 64,
            mailbox_limit: 512,
        }
    }
}
/// JobHost：一个 actor 的调度面。
pub struct JobHost {
    pub actor_id: String,
    pub inbox: crate::mail::Inbox,
    pub patterns: Vec<crate::pattern::Pattern>,
    pub make_job: Box<JobFactory>,
}
/// Caller：代表调用方宿主，持有回信 Inbox 与 active 上限。
/// Runtime 内部用它发起 admit；对外暴露供调用方使用。
pub struct Caller {
    pub actor_id: String,
    pub active_limit: usize,
}
impl Caller {
    pub fn new(actor_id: impl Into<String>, active_limit: usize) -> Self {
        Self {
            actor_id: actor_id.into(),
            active_limit,
        }
    }
    /// 构造一次调用的 Call（与 Runtime.ask 内部同形）。
    pub fn make_call(
        &self,
        topic: impl Into<String>,
        payload: Value,
        timeout_ms: Option<u64>,
        slot: CompletionSlot,
    ) -> Call {
        Call::new(topic, payload, timeout_ms, slot)
    }
}
/// 完成槽：Pending 轮询它兑现。
#[derive(Clone)]
pub struct CompletionSlot {
    inner: Rc<RefCell<Option<Completion>>>,
}
impl CompletionSlot {
    pub fn new() -> Self {
        Self {
            inner: Rc::new(RefCell::new(None)),
        }
    }
    pub fn settle(&self, completion: Completion) {
        let mut slot = self.inner.borrow_mut();
        if slot.is_none() {
            *slot = Some(completion);
        }
    }
    pub fn take(&self) -> Option<Completion> {
        self.inner.borrow_mut().take()
    }
    pub fn peek(&self) -> Option<Completion> {
        self.inner.borrow().clone()
    }
}
impl Default for CompletionSlot {
    fn default() -> Self {
        Self::new()
    }
}
/// pay 回调的共享槽（askWith 结算时恰好触发一次）。
type PaySlot = Rc<RefCell<Option<Box<dyn FnOnce(Completion)>>>>;
/// Call：一次外部调用的可变状态（放进 sys.admit 的 payload）。
#[derive(Clone)]
pub struct Call {
    pub from: String,
    pub topic: String,
    pub payload: Value,
    pub timeout_ms: Option<u64>,
    pub request_id: Rc<RefCell<Option<EnvelopeId>>>,
    pub cancelled: Rc<Cell<bool>>,
    pub slot: CompletionSlot,
    /// askWith 的 pay 回调：结算时恰好调用一次。
    pay: PaySlot,
}
impl Call {
    pub fn new(
        topic: impl Into<String>,
        payload: Value,
        timeout_ms: Option<u64>,
        slot: CompletionSlot,
    ) -> Self {
        Self::new_from("caller", topic, payload, timeout_ms, slot)
    }
    /// 带发信方身份的构造。
    pub fn new_from(
        from: impl Into<String>,
        topic: impl Into<String>,
        payload: Value,
        timeout_ms: Option<u64>,
        slot: CompletionSlot,
    ) -> Self {
        Self {
            topic: topic.into(),
            payload,
            timeout_ms,
            from: from.into(),
            request_id: Rc::new(RefCell::new(None)),
            cancelled: Rc::new(Cell::new(false)),
            slot,
            pay: Rc::new(RefCell::new(None)),
        }
    }
    /// 设置 pay 回调（askWith 用）。
    pub fn on_pay(&self, pay: Box<dyn FnOnce(Completion)>) {
        *self.pay.borrow_mut() = Some(pay);
    }
    pub fn settle(&self, completion: Completion) {
        self.slot.settle(completion.clone());
        // 回调只触发一次：take 后调用，避免重入结算再次触发。
        let pay = self.pay.borrow_mut().take();
        if let Some(pay) = pay {
            pay(completion);
        }
    }
}
/// Wait Job：处理 sys.admit，发起 ask 并在回信到达时结算 Call。
pub struct WaitHooks {
    call: Call,
}
impl WaitHooks {
    pub fn new(call: Call) -> Self {
        Self { call }
    }
    pub fn call(&self) -> &Call {
        &self.call
    }
}
impl JobHooks for WaitHooks {
    fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
        if self.call.cancelled.get() {
            self.call.settle(Completion {
                request_id: ctx.root().header.id.clone(),
                body: Some(err_payload("cancelled")),
            });
            return Vec::new();
        }
        let effect = ctx
            .ask_from(
                self.call.from.clone(),
                "reply",
                self.call.topic.clone(),
                self.call.payload.clone(),
                self.call.timeout_ms,
            )
            .expect("wait ask must build");
        if let Effect::Ask { intent } = &effect {
            *self.call.request_id.borrow_mut() = Some(intent.request.header.id.clone());
        }
        vec![effect]
    }
    fn on_stage_result(
        &self,
        _ctx: &mut Context,
        _stage: &str,
        completion: Completion,
    ) -> Vec<Effect> {
        self.call.settle(completion);
        Vec::new()
    }
    fn on_finished(&self, body: Option<Value>) -> Vec<Effect> {
        // 超时/无答案为 None，也要结算 Completion(body=None) 让 Pending.ready resolve。
        self.call.settle(Completion {
            request_id: self.call.request_id.borrow().clone().unwrap_or_default(),
            body,
        });
        Vec::new()
    }
}
/// 通用 finish Job：hook 直接 finish 一个 ok 结果。
pub struct FinishJob(pub Value);
impl JobHooks for FinishJob {
    fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
        let _ = ctx;
        vec![Effect::Finish {
            body: ok_payload(self.0.clone()),
        }]
    }
}
