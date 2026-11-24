//! Core：一个 host 的状态机与四相 pump。
//!
//! 四相：READ（收信/resume）→ TIMER（到期唤醒）→ WORK（adopt/hook）→ DELIVERY（flush）。
//! 续跑读 live clock：延迟回调在 turn 外运行，必须用 core.clock 而非 pinned clock。
use crate::envelope::{
    make_envelope, Address, Completion, Envelope, EnvelopeId, IdGen, MakeEnvelope,
};
use crate::errors::{EbaError, Result};
use crate::job::effect::Effect;
use crate::job::job::Context;
use crate::job::job::{Call, Job, JobHooks, Lifecycle, WaitHooks};
use crate::job::lease::Lease;
use crate::job::lease_registry::LeaseRegistry;
use crate::mail::Bus;
use crate::notify::Signal;
use crate::prelude::*;
use crate::record::RecordLog;
use crate::time::{safe_deadline, Clock};
use crate::value::{err_payload, err_payload_detail, Value};
/// 统一执行 hook，Err 时标记 finished 并生成 job_failed 结算。
/// 借用安全：ctx 只借 job.state，返回后立即释放，再借整个 job 生成失败 effects。
fn run_hook<F>(
    job: &mut Job,
    actor_id: String,
    reply_to: String,
    id_gen: &dyn IdGen,
    hook: F,
) -> (Vec<Effect>, Option<String>)
where
    F: FnOnce(&dyn JobHooks, &mut Context) -> core::result::Result<Vec<Effect>, String>,
{
    let (result, builder_error) = {
        let mut ctx = Context::new(&mut job.state, actor_id, reply_to, id_gen);
        let result = hook(job.hooks.as_ref(), &mut ctx);
        let error = ctx.error.borrow().clone();
        (result, error)
    };
    // builder 期 throw 可捕获；RS 经错误槽转为统一 job_failed 结算。
    let result = match result {
        Ok(_) if builder_error.is_some() => Err(builder_error.unwrap()),
        other => other,
    };
    match result {
        Ok(effects) => (effects, None),
        Err(message) => {
            if job.state.lifecycle != Lifecycle::Finished {
                job.state.transition(Lifecycle::Finished);
            }
            let mut detail = BTreeMap::new();
            detail.insert("message".to_string(), Value::Str(message.clone()));
            let body = err_payload_detail("job_failed", Value::Map(detail));
            // 清理效果只经 applyFinished 落地（tell/record/close），
            // 由 Effect::Finish 分支统一调用 notify_finished 完成，避免非法清理效果被真实执行。
            (vec![Effect::Finish { body }], Some(message))
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostState {
    Running,
    Draining,
    Closing,
    Closed,
}
pub struct TimerEntry {
    owner: EnvelopeId,
    deadline: u64,
}
pub struct Core {
    pub actor_id: String,
    pub signal: Rc<Signal>,
    pub sub: crate::mail::Inbox,
    pub make_job: Box<crate::job::job::JobFactory>,
    pub queue: VecDeque<Envelope>,
    pub jobs: BTreeMap<EnvelopeId, Job>,
    pub held: BTreeMap<EnvelopeId, Lease>,
    pub leases: LeaseRegistry,
    pub records: RecordLog,
    pub mailbox: crate::job::mailbox::Mailbox,
    pub buried: Vec<Envelope>,
    pub timers: Vec<TimerEntry>,
    /// barrier/timer 之后延迟重放的 effect 段。
    pub pending_post: BTreeMap<EnvelopeId, Vec<Effect>>,
    /// barrier 等待的 durable 水位；到达后重放 post。
    pub barrier_watermark: BTreeMap<EnvelopeId, u64>,
    /// revoke 时 job 尚不可取消（非 active / held），
    /// 记下 id 待后续轮次重试，避免丢失取消意图。
    pub cancel_pending: BTreeSet<EnvelopeId>,
    pub state: HostState,
    pub phase: usize,
    pub clock: Option<Rc<dyn Clock>>,
    pub work_budget: usize,
    pub queue_limit: usize,
    pub active_limit: usize,
    /// host 默认请求超时。
    pub timeout: Option<u64>,
    pub rejected_count: usize,
    pub abandoned: Vec<Envelope>,
    pub close_requested: bool,
    pub id_gen: Rc<dyn IdGen>,
    pub reply_to: String,
    pub host_id: u64,
    /// hook 失败：结算仍落地，但错误沿 advance 传播。
    pub pending_error: Option<crate::errors::EbaError>,
}
impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        actor_id: impl Into<String>,
        inbox: crate::mail::Inbox,
        make_job: Box<crate::job::job::JobFactory>,
        signal: Rc<Signal>,
        records: RecordLog,
        id_gen: Rc<dyn IdGen>,
        host_id: u64,
    ) -> Self {
        Self {
            actor_id: actor_id.into(),
            signal,
            sub: inbox,
            make_job,
            queue: VecDeque::new(),
            jobs: BTreeMap::new(),
            held: BTreeMap::new(),
            leases: LeaseRegistry::new(),
            records,
            mailbox: crate::job::mailbox::Mailbox::new(512),
            buried: Vec::new(),
            timers: Vec::new(),
            pending_post: BTreeMap::new(),
            barrier_watermark: BTreeMap::new(),
            cancel_pending: BTreeSet::new(),
            state: HostState::Running,
            phase: 0,
            clock: None,
            work_budget: 64,
            queue_limit: 512,
            active_limit: 256,
            timeout: None,
            rejected_count: 0,
            abandoned: Vec::new(),
            close_requested: false,
            id_gen,
            reply_to: String::new(),
            host_id,
            pending_error: None,
        }
    }
    pub fn attach_clock(&mut self, clock: Rc<dyn Clock>) {
        self.clock = Some(clock);
    }
    pub fn live_now(&self, pinned: u64) -> u64 {
        self.clock.as_ref().map(|c| c.now_ms()).unwrap_or(pinned)
    }
    pub fn next_timer_ms(&self) -> Option<u64> {
        self.timers.iter().map(|t| t.deadline).min()
    }
    pub fn next_held_deadline_ms(&self) -> Option<u64> {
        // 537-546：跳过已 closed/settled 的 held 租约，
        // 避免陈旧 deadline 持续武装 agenda。
        self.held
            .values()
            .filter(|l| !l.is_closed() && !l.is_settled())
            .filter_map(|l| l.deadline_ms())
            .min()
    }
    pub fn next_deadline_ms(&self) -> Option<u64> {
        [
            self.leases.next_deadline_ms(),
            self.next_held_deadline_ms(),
            self.next_timer_ms(),
        ]
        .into_iter()
        .flatten()
        .min()
    }
    /// 是否需要推进：入队/定时/租约都会 notify 对应旗标，因此这是一个
    /// 按位与判据，而不是遍历容器。空闲时全部宿主一次跳过。
    pub fn wants_pump(&self) -> bool {
        self.signal.has(crate::notify::READ)
            || self.signal.has(crate::notify::TIMER)
            || self.signal.has(crate::notify::WORK)
            || self.signal.has(crate::notify::DELIVERY)
            || !self.queue.is_empty()
            || !self.mailbox.is_empty()
    }
    pub fn has_work(&self) -> bool {
        !self.queue.is_empty()
            || !self.jobs.is_empty()
            || !self.held.is_empty()
            || self.leases.size() > 0
            || !self.mailbox.is_empty()
            || !self.timers.is_empty()
    }
    pub fn is_quiescent(&self) -> bool {
        !self.has_work() && self.state != HostState::Running
    }
    pub fn begin_drain(&mut self) {
        if self.state == HostState::Running {
            self.state = HostState::Draining;
        }
    }
    fn mint(&self) -> u64 {
        // 所有权用 EnvelopeId 比较，mint 仅作占位符。
        self.host_id
    }
    /// READ 相：result 走 resume，其余进 queue。
    pub fn phase_read(&mut self, bus: &Bus, pinned: u64) -> bool {
        let receiver = self.sub.receiver();
        if !receiver.borrow() {
            return false;
        }
        let env = receiver.try_recv();
        receiver.unborrow();
        let Some(env) = env else { return false };
        if env.header.address.kind() == "diagnostic" {
            // diagnostic 不入 job 队列，不触发 adopt。
            return true;
        }
        if env.header.address.kind() == "result" {
            self.resume(bus, &env, pinned);
        } else if self.queue.len() < self.queue_limit {
            self.queue.push_back(env);
        } else {
            // reject(...,"queue_full") 必须回信。
            self.reject_root(bus, &env, "queue_full");
            self.rejected_count += 1;
        }
        true
    }
    /// WORK 相：adopt + begin。
    pub fn phase_work(&mut self, bus: &Bus, pinned: u64) -> bool {
        let Some(root) = self.queue.pop_front() else {
            return false;
        };
        // refused(core)：非 running 宿主的新 root 一律拒绝，不 adopt。
        if self.state != HostState::Running {
            self.reject_root(bus, &root, "host_draining");
            self.rejected_count += 1;
            return true;
        }
        if self.jobs.len() >= self.active_limit {
            self.reject_root(bus, &root, "active_full");
            self.rejected_count += 1;
            return true;
        }
        let root_id = root.header.id.clone();
        // 重复 root 在 makeJob 之前就拒绝（可捕获），
        // 不能先跑一遍 hook 再 panic=abort。
        if self.jobs.contains_key(&root_id) {
            self.reject_root(bus, &root, "adoption_failed");
            self.rejected_count += 1;
            return true;
        }
        let is_request = root.header.address.kind() == "request";
        let hooks = (self.make_job)(&root);
        let mut job = Job::new(root, hooks, 100);
        // adopt 时 New → Active。不推进会让
        // revoke 的 lifecycle==Active 判定恒假，I2 取消结算全部失效。
        job.state.transition(Lifecycle::Active);
        let reply_to = self.reply_to.clone();
        let (effects, hook_error) = {
            run_hook(
                &mut job,
                self.actor_id.clone(),
                reply_to,
                self.id_gen.as_ref(),
                |hooks, ctx| {
                    let returned = hooks.try_begin(ctx)?;
                    Ok(ctx.collect(returned))
                },
            )
        };
        // request 的 job 没有任何 effect（含未显式 close）时，
        // 必须回 job_silent，不能静默丢弃调用方的等待。
        let effects = if effects.is_empty() && is_request && !self.suppress_silent(&root_id) {
            vec![Effect::Finish {
                body: err_payload("job_silent"),
            }]
        } else {
            effects
        };
        self.jobs.insert(root_id.clone(), job);
        let _ = self.mint();
        self.apply_effects(&root_id, effects, bus, pinned);
        if hook_error.is_some() {
            self.pending_error = hook_error.map(crate::errors::EbaError::Other);
        } else {
            // commitTo：收尾无 reply 且无在途租约时形成 Control → silence→notify→drop。
            // 覆盖只 tell/record 的非空 effect 列表（原实现只处理空列表）。
            self.finalize_if_idle(&root_id, bus, pinned);
        }
        true
    }
    /// TIMER 相：到期 timer 触发 onResume（读 live clock）。
    pub fn phase_timer(&mut self, bus: &Bus, pinned: u64) -> bool {
        let now = self.live_now(pinned);
        let due: Vec<EnvelopeId> = self
            .timers
            .iter()
            .filter(|t| t.deadline <= now)
            .map(|t| t.owner.clone())
            .collect();
        if due.is_empty() {
            return false;
        }
        self.timers.retain(|t| t.deadline > now);
        for owner in due {
            // 若有延迟的 post 段，重放它（透传 mint 后的 effect 已带正确 requestId）；
            // 否则回退到 onResume 钩子。
            if !self.replay_timer_post(&owner, bus, pinned) {
                if let Some(job) = self.jobs.get_mut(&owner) {
                    let reply_to = self.reply_to.clone();
                    let (effects, hook_error) = {
                        run_hook(
                            job,
                            self.actor_id.clone(),
                            reply_to,
                            self.id_gen.as_ref(),
                            |hooks, ctx| {
                                let returned = hooks.try_on_resume(ctx)?;
                                Ok(ctx.collect(returned))
                            },
                        )
                    };
                    self.apply_effects(&owner, effects, bus, pinned);
                    self.finalize_if_idle(&owner, bus, pinned);
                    if hook_error.is_some() {
                        self.pending_error = hook_error.map(crate::errors::EbaError::Other);
                    }
                }
            }
        }
        true
    }
    /// DELIVERY 相：逐封重试 parked 信（park→rearm→bury 背压状态机）。
    pub fn phase_delivery(&mut self, bus: &Bus) -> bool {
        if self.mailbox.is_empty() {
            return false;
        }
        let mut progressed = false;
        // 每轮最多 flush 若干次，受 mailbox 当前规模限制（有限推进）。
        let budget = self.mailbox.len().min(self.work_budget);
        for _ in 0..budget {
            let (step, _sent) = self.mailbox.flush_one(bus);
            // 二次失败的信由 Mailbox 内部埋葬，交本 core 统一结算 delivery_failed。
            self.buried.append(&mut self.mailbox.take_buried());
            match step {
                crate::job::mailbox::Step::Advanced => {
                    progressed = true;
                }
                crate::job::mailbox::Step::Checked => {
                    // 仍 parked，未推进；等下一次 space 信号 rearm。
                }
                crate::job::mailbox::Step::Idle => break,
            }
        }
        progressed
    }
    /// 到期租约在本 core 内恢复 owner job（    /// `resume(core, ..., source=number)`：expireDue → onStageResult → applyEffects）。
    /// 不能只回传 completion 交给 front 用 on_finished 结算——多阶段 job 需要 stage
    /// 才能正确处理超时，且 owner job 属于各自 host 而非 front。
    pub fn expire_and_resume(&mut self, bus: &Bus, pinned: u64) -> bool {
        let now = self.live_now(pinned);
        let mut claims = Vec::new();
        while let Some((owner, stage, completion)) = self.leases.expire_due(now) {
            claims.push((owner, stage, completion));
        }
        let progressed = !claims.is_empty();
        for (owner, stage, completion) in claims {
            let mut retired = false;
            if let Some(job) = self.jobs.get_mut(&owner) {
                let reply_to = self.reply_to.clone();
                let (effects, hook_error) = {
                    run_hook(
                        job,
                        self.actor_id.clone(),
                        reply_to,
                        self.id_gen.as_ref(),
                        |hooks, ctx| {
                            let returned = hooks.try_on_stage_result(ctx, &stage, completion)?;
                            Ok(ctx.collect(returned))
                        },
                    )
                };
                self.apply_effects(&owner, effects, bus, pinned);
                if hook_error.is_some() {
                    self.pending_error = hook_error.map(crate::errors::EbaError::Other);
                }
                retired = self.leases.count(&owner) == 0;
            }
            if retired {
                self.remove_job(&owner);
            }
        }
        progressed
    }
    /// 被埋信件（两次投递失败）必须结算 delivery_failed 并释放等待者，
    /// 否则调用方永久挂起。
    pub fn settle_buried(&mut self, bus: &Bus, pinned: u64) -> (bool, Vec<Envelope>) {
        let buried = core::mem::take(&mut self.buried);
        // 只有真正产生结算/回投才算进展，非 result 门控信件不算。
        let mut progressed = false;
        let mut unhandled = Vec::new();
        for letter in buried {
            // 只对 result 信封生成失败结算。
            if letter.header.address.kind() != "result" {
                continue;
            }
            let Some(request_id) = letter.correlation_id().cloned() else {
                continue;
            };
            let Some((owner, stage)) = self.leases.take_by_id(&request_id) else {
                // 本地无人持有 → 交 runtime 回投 bus（answerSpent 的兜底）。
                unhandled.push(letter);
                progressed = true;
                continue;
            };
            progressed = true;
            let completion = Completion {
                request_id: request_id.clone(),
                body: Some(err_payload("delivery_failed")),
            };
            let mut retired = false;
            if let Some(job) = self.jobs.get_mut(&owner) {
                let reply_to = self.reply_to.clone();
                let (effects, hook_error) = {
                    run_hook(
                        job,
                        self.actor_id.clone(),
                        reply_to,
                        self.id_gen.as_ref(),
                        |hooks, ctx| {
                            let returned = hooks.try_on_stage_result(ctx, &stage, completion)?;
                            Ok(ctx.collect(returned))
                        },
                    )
                };
                self.apply_effects(&owner, effects, bus, pinned);
                if hook_error.is_some() {
                    self.pending_error = hook_error.map(crate::errors::EbaError::Other);
                }
                retired = self.leases.count(&owner) == 0;
            }
            if retired {
                self.remove_job(&owner);
            }
        }
        (progressed, unhandled)
    }
    /// 透出被取消的 requestId，供 Runtime 跨 host 传播 revoke（I2）。
    pub fn take_cancelled(&mut self) -> Vec<EnvelopeId> {
        self.leases.take_cancelled()
    }
    /// 对一组 requestId 撤销本 host 的 queued/held/active job。
    /// 按 requestId 撤销 queued / held / active 三类 job。
    /// 撤销 active job 时调用 on_finished(errPayload("cancelled")) 并清理租约，
    /// 使 Wait 的 Completion 兑现、宿主能收敛到 quiescent。
    pub fn revoke(&mut self, request_ids: &[EnvelopeId], bus: &Bus, pinned: u64) -> bool {
        self.revoke_with(request_ids, bus, pinned, "cancelled")
    }
    /// 带显式原因的 revoke（host_closing / cancelled）。
    fn revoke_with(
        &mut self,
        request_ids: &[EnvelopeId],
        bus: &Bus,
        pinned: u64,
        reason: &str,
    ) -> bool {
        let mut progressed = false;
        // queued：队列 + inbox 双清。
        if !request_ids.is_empty() {
            let before = self.queue.len();
            self.queue.retain(|e| !request_ids.contains(&e.header.id));
            progressed |= self.queue.len() != before;
            for id in request_ids {
                progressed |= self.sub.remove_by_id(id);
            }
        }
        // held：未结算的回信槽 revoke("cancelled")——cancel 写 body 后
        // resolve_held 能取回并结算 onFinished。
        for id in request_ids {
            if let Some(lease) = self.held.get(id) {
                if !lease.is_closed() && lease.peek().is_none() {
                    let _ = lease.revoke(reason);
                    progressed = true;
                }
            }
        }
        // active：
        // 可取消的结算 cancelled 并撤销其下游租约。
        for id in request_ids {
            match self.jobs.get_mut(id) {
                None => {
                    self.cancel_pending.remove(id);
                }
                Some(job) if job.state.lifecycle != Lifecycle::Active || job.state.held => {
                    self.cancel_pending.remove(id); // 同样从 pending 移除（未取消的是终止态）
                }
                Some(job) => {
                    job.state.notified = true; // 本路径已通知，避免 Finish effect 二次触发
                    let effects = match job.hooks.try_on_finished(Some(err_payload(reason))) {
                        Ok(e) => e,
                        Err(_) => {
                            job.state.transition(Lifecycle::Finished);
                            vec![Effect::Finish {
                                body: err_payload("job_failed"),
                            }]
                        }
                    };
                    self.leases.cancel(id);
                    // cancelJob：onFinished 返回值只经 applyFinished（tell/record/close）。
                    self.apply_finished_effects(id, effects, bus, pinned);
                    self.remove_job(id);
                    progressed = true;
                }
            }
        }
        progressed
    }
    /// 快速判空：本 core 是否有待传播的取消意图（租约到期/主动撤销 或 cancelPending）。
    /// 供 Runtime 空转时短路整段取消收集，避免每轮无谓的遍历与容器触碰。
    pub fn has_cancellations(&self) -> bool {
        self.leases.has_cancelled() || !self.cancel_pending.is_empty()
    }
    /// 取出并清空待取消集合（空集零分配）。
    pub fn take_pending_cancels(&mut self) -> Vec<EnvelopeId> {
        if self.cancel_pending.is_empty() {
            return Vec::new();
        }
        core::mem::take(&mut self.cancel_pending)
            .into_iter()
            .collect()
    }
    /// 退役前结算在途 job、排空记录、清 timer 与信号，
    /// 返回未能结算的 abandoned 信封。防止丢在途 ending、待落盘记录和 timer。
    pub fn close_and_collect(&mut self, bus: &Bus, pinned: u64) -> Result<Vec<Envelope>> {
        // runtime.close → closeHost(settle=false) → finishJobs reason=host_draining。
        self.close_and_collect_with(bus, pinned, "host_draining")
    }
    /// 带显式结算原因的 close（供 Runtime 区分 drain/close）。
    pub fn close_and_collect_with(
        &mut self,
        bus: &Bus,
        pinned: u64,
        reason: &str,
    ) -> Result<Vec<Envelope>> {
        let mut abandoned = Vec::new();
        // 在途 job：
        let ids: Vec<EnvelopeId> = self.jobs.keys().cloned().collect();
        for id in &ids {
            if let Some(job) = self.jobs.get(id) {
                // 仅 notice=open 时触发，setNotice 后不重复。
                if !job.state.notified {
                    let root = job.state.root.clone();
                    job.hooks.on_rejected(&root, reason);
                }
                // finishJobs：每个在途 job 的 root 都作为 abandoned 返回，
                // 调用方据此恢复未结算请求（revoke_with 内部只结算不返回）。
                abandoned.push(job.state.root.clone());
            }
        }
        // 先 closeHeld：held 租约恒 revoke("host_closing")，
        // 与随后 finishJobs 的 reason 相互独立。顺序颠倒会让 revoke_with
        // 先把 body 写成 host_draining，held 分支再因 peek().is_some() 跳过。
        for lease in self.held.values() {
            if !lease.is_closed() {
                if lease.peek().is_none() {
                    let _ = lease.revoke("host_closing");
                }
                lease.close(); // reply.close()
            }
        }
        self.held.clear();
        // 清除所有 job 的 held 位，避免残留 work 判定。
        for job in self.jobs.values_mut() {
            job.state.held = false;
        }
        if !ids.is_empty() {
            self.revoke_with(&ids, bus, pinned, reason);
        }
        // closeHost 的 leak guard：recover 后仍有未释放/滞留租约则拒绝关闭。
        self.leases.recover();
        if self.leases.unreleased() > 0 || self.leases.size() > 0 {
            return Err(crate::errors::EbaError::LeasePending(
                "lease release pending at close".into(),
            ));
        }
        // 记录环 staging → sink（Record rule 6：log 是唯一恢复事实）。
        // fail-closed：落盘失败时提前返回，host 保持未 Closed 以便重试。
        self.records.settle()?;
        // 邮箱与队列中的僵信交还调用方。
        abandoned.append(&mut self.mailbox.drain());
        abandoned.extend(core::mem::take(&mut self.queue));
        // settleInbound：inbox 中未消费的信也要抽干交还，
        // 否则退役 host 的入站僵信被静默丢弃。
        {
            let rx = self.sub.receiver();
            while let Some(env) = rx.try_recv() {
                abandoned.push(env);
            }
        }
        // 清 timer 与信号，避免退役 host 残留唤醒。
        self.timers.clear();
        self.pending_post.clear();
        self.barrier_watermark.clear();
        self.cancel_pending.clear();
        self.signal.clear(
            crate::notify::READ
                | crate::notify::TIMER
                | crate::notify::WORK
                | crate::notify::DELIVERY,
        );
        self.state = HostState::Closed;
        abandoned.append(&mut self.abandoned);
        Ok(abandoned)
    }
    /// 统一回收 job：
    /// 1. `seal` 走 Delivering 中间态（Active→Delivering→Finished），不再跳过；
    /// 2. `seal` 撤销本 job 的在途租约；
    /// 3. `drop` 撤销 timer 与延迟 post/barrier 段，避免死 job 持续武装 agenda。
    fn remove_job(&mut self, id: &EnvelopeId) {
        if let Some(job) = self.jobs.get_mut(id) {
            if job.state.lifecycle == Lifecycle::Active {
                job.state.transition(Lifecycle::Delivering);
            }
            if job.state.lifecycle == Lifecycle::Delivering {
                job.state.transition(Lifecycle::Finished);
            }
        }
        // seal(): core.leases.cancel(rootId)——终结 job 不得残留在途租约。
        self.leases.cancel(id);
        // drop(): core.cancelTimers(requestId)。
        self.timers.retain(|t| &t.owner != id);
        self.pending_post.remove(id);
        self.barrier_watermark.remove(id);
        self.cancel_pending.remove(id);
        self.jobs.remove(id);
    }
    pub fn pending_cancels_len(&self) -> usize {
        self.cancel_pending.len()
    }
    pub fn mailbox_len(&self) -> usize {
        self.mailbox.len()
    }
    pub fn charge_leases(&mut self, delta: u64) {
        // 已结算/已关闭的租约不再扣预算。
        for lease in self.held.values() {
            if lease.is_closed() || lease.is_settled() {
                continue;
            }
            if !lease.consume(delta) {
                let _ = lease.revoke("budget");
            }
        }
    }
    pub fn expire_held(&mut self, pinned: u64) {
        let now = self.live_now(pinned);
        // 已关闭租约不参与过期扫描。
        for lease in self.held.values() {
            if lease.is_closed() {
                continue;
            }
            if lease.expired(now) {
                let _ = lease.revoke("deadline");
            }
        }
    }
    fn resume(&mut self, bus: &Bus, result: &Envelope, pinned: u64) {
        let claim = match self.leases.take_result(result) {
            Ok(claim) => claim,
            Err(e) => {
                // UnreadableResult → keepUnread → keepAbandoned；
                // 信封保留供 close 归还，租约不解绑。其它错误同样不伪造结算。
                self.abandoned.push(result.clone());
                if !matches!(e, EbaError::UnreadableResult(_)) {
                    self.pending_error.get_or_insert(e);
                }
                return;
            }
        };
        if let Some((owner, stage, completion)) = claim {
            let mut retired = false;
            if let Some(job) = self.jobs.get_mut(&owner) {
                let reply_to = self.reply_to.clone();
                let (effects, hook_error) = {
                    run_hook(
                        job,
                        self.actor_id.clone(),
                        reply_to,
                        self.id_gen.as_ref(),
                        |hooks, ctx| {
                            let returned = hooks.try_on_stage_result(ctx, &stage, completion)?;
                            Ok(ctx.collect(returned))
                        },
                    )
                };
                self.apply_effects(&owner, effects, bus, pinned);
                if hook_error.is_some() {
                    self.pending_error = hook_error.map(crate::errors::EbaError::Other);
                }
                // WaitHooks 一类"只结算 Call、不产 Finish"的 job 既不置 replied 也不置
                // Finished；它们的租约已在本函数开头被 take_result 摘除。判据只能是
                // "已无在途租约"——此时该 job 不可能再被 resume 唤醒，属于终结态。
                // 若不回收，`front.jobs` 会随每次 ask 线性膨胀（实测 +5 KB/syscall，
                // 20k 次后 RSS 104 MB），且 `has_work()` 恒为真使系统永不静止。
                retired = self.leases.count(&owner) == 0;
            }
            if retired {
                self.remove_job(&owner);
            }
        }
    }
    /// held 租约 body 就绪后调用 onFinished 结算（含 Wait 的 Call）。
    pub fn resolve_held(&mut self, bus: &Bus, pinned: u64) -> bool {
        // `reply.take() !== null` 才结算。cancel 写入 errPayload
        // 后 revoke 的 held 租约会带 body，这里即可取回；仅 closed 而无 body 不再触发。
        let ready: Vec<EnvelopeId> = self
            .held
            .iter()
            .filter(|(_, l)| l.peek().is_some())
            .map(|(id, _)| id.clone())
            .collect();
        let mut progressed = false;
        for id in ready {
            if let Some(lease) = self.held.remove(&id) {
                let body = lease.take();
                // 不提前移除 job：apply_effects 的 make_result 需要从 job.root 取
                // reply_to，才能在 hold 完成后把回信发到真正的发问宿主。
                if let Some(job) = self.jobs.get_mut(&id) {
                    job.state.held = false; // markHeld(state, false)
                }
                // resolveHeld 自己用 replyFor + deliverEnding(Control(body))
                // 发回信，再 notify(onFinished)。回信不再依赖 hook 返回 Finish effect。
                let reply_body = body.clone().unwrap_or_else(|| err_payload("no body"));
                let result = self.make_result(&id, reply_body.clone());
                self.settle_reply(bus, result);
                if let Some(job) = self.jobs.get_mut(&id) {
                    job.state.replied = true;
                    job.state.reply_body = Some(reply_body.clone());
                    job.state.notified = true; // 本路径已通知，避免 Finish effect 二次触发
                    let effects = match job.hooks.try_on_finished(body) {
                        Ok(e) => e,
                        Err(_) => {
                            job.state.transition(Lifecycle::Finished);
                            Vec::new()
                        }
                    };
                    // applyFinished：严格白名单（tell/record/close），Finish 也被拒绝。
                    self.apply_finished_effects(&id, effects, bus, pinned);
                    self.remove_job(&id);
                    progressed = true;
                }
            }
        }
        progressed
    }
    /// 落地前校验 effect 顺序与冲突。
    /// 非法组合必须抛错，不能静默生效。
    fn validate_effects(effects: &[Effect]) -> Result<()> {
        // assertNoDeclaredAnswer 只看 reply，assertAskAllowed 只看
        // control/lifecycle（不因 reply 而拒绝 ask）。panic=abort 下必须返回 Err。
        let mut finished = false;
        let mut replied = false;
        let mut held = false;
        for (index, effect) in effects.iter().enumerate() {
            match effect {
                Effect::Barrier { .. } | Effect::Timer { .. } => {
                    if index + 1 < effects.len() {
                        return Err(EbaError::Other(
                            "barrier/timer must be the last effect".into(),
                        ));
                    }
                }
                Effect::Hold { .. } => {
                    // 前一个非 hold → "cannot hold after other effects"；
                    // 连续 hold 由紧随其后的 assertHoldAllowed 抛 "job already held"。
                    if index != 0 {
                        if matches!(effects[index - 1], Effect::Hold { .. }) {
                            return Err(EbaError::Other("job already held".into()));
                        }
                        return Err(EbaError::Other("cannot hold after other effects".into()));
                    }
                    held = true;
                }
                Effect::Reply { body } => {
                    // putReply：held 后再 reply 必须抛错。
                    if held {
                        return Err(EbaError::Other("job already held".into()));
                    }
                    if body != &crate::value::Value::Null && !crate::value::has_result_shape(body) {
                        return Err(EbaError::Other("invalid result body".into()));
                    }
                    replied = true;
                }
                Effect::Finish { body } => {
                    // finishControl：held 后再 finish 必须抛错。
                    if held {
                        return Err(EbaError::Other("job already held".into()));
                    }
                    // finish 的 body 同样经 hasResultShape。
                    if body != &crate::value::Value::Null && !crate::value::has_result_shape(body) {
                        return Err(EbaError::Other("invalid result body".into()));
                    }
                    finished = true;
                    // finishControl：带 body 的 finish 同时 putReply。
                    if body != &crate::value::Value::Null {
                        replied = true;
                    }
                }
                Effect::Settle { .. } | Effect::Revoke { .. } => {
                    if replied {
                        return Err(EbaError::Other("root already replied".into()));
                    }
                }
                Effect::Ask { .. } => {
                    // assertAskAllowed 不检查 reply：reply + ask 是合法组合。
                    if finished {
                        return Err(EbaError::Other("cannot ask after finish".into()));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
    /// 的跨轮前置：held / request 根 / lifecycle / replied /
    /// 在途租约 / maxInflight。返回可捕获 Err（panic=abort 下不能 panic）。
    fn validate_turn(&self, job_id: &EnvelopeId, effects: &[Effect]) -> Result<()> {
        let Some((already_held, max_inflight)) = self
            .jobs
            .get(job_id)
            .map(|j| (j.state.held, j.state.max_inflight))
        else {
            return Ok(());
        };
        let mut held_in_turn = false;
        let mut asks = self.leases.count(job_id);
        let mut finished = false;
        let mut replied = false;
        for e in effects {
            match e {
                Effect::Hold { .. } => {
                    if held_in_turn || already_held {
                        return Err(EbaError::Other("job already held".into()));
                    }
                    let Some(job) = self.jobs.get(job_id) else {
                        return Ok(());
                    };
                    if !matches!(&job.state.root.header.address, Address::Request { .. }) {
                        return Err(EbaError::Other("root is not a request".into()));
                    }
                    if job.state.lifecycle != Lifecycle::Active {
                        return Err(EbaError::Other("job already finished".into()));
                    }
                    if job.state.replied {
                        return Err(EbaError::Other("root already replied".into()));
                    }
                    if self.leases.count(job_id) > 0 {
                        return Err(EbaError::Other("cannot hold with pending requests".into()));
                    }
                    held_in_turn = true;
                }
                Effect::Ask { .. } => {
                    if held_in_turn {
                        return Err(EbaError::Other("cannot ask after hold".into()));
                    }
                    if finished {
                        return Err(EbaError::Other("cannot ask after finish".into()));
                    }
                    if asks >= max_inflight {
                        return Err(EbaError::Other("maxInflight exhausted".into()));
                    }
                    asks += 1;
                }
                Effect::Reply { body } => {
                    // 跨轮门控：本轮 hold 或历史 held 后再 reply/finish 一律拒绝，
                    // 与 validate_effects 同轮门控互补（putReply/finishControl）。
                    if held_in_turn || already_held {
                        return Err(EbaError::Other("job already held".into()));
                    }
                    if body != &crate::value::Value::Null && !crate::value::has_result_shape(body) {
                        return Err(EbaError::Other("invalid result body".into()));
                    }
                    replied = true;
                }
                Effect::Finish { body } => {
                    if held_in_turn || already_held {
                        return Err(EbaError::Other("job already held".into()));
                    }
                    if body != &crate::value::Value::Null && !crate::value::has_result_shape(body) {
                        return Err(EbaError::Other("invalid result body".into()));
                    }
                    finished = true;
                    if body != &crate::value::Value::Null {
                        replied = true;
                    }
                }
                Effect::Settle { .. } | Effect::Revoke { .. } => {
                    if replied {
                        return Err(EbaError::Other("root already replied".into()));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
    /// 终结 job 前触发一次 onFinished。
    /// `notified` 旗标保证错误/撤销/正常终结三条路径只会触发一次。
    /// 返回的清理效果按 applyFinished 约束：仅 tell/record/close 可落地；
    /// 兼容 RS 既有的 onFinished→Finish 回信写法（holds test 依赖）。
    fn notify_finished(
        &mut self,
        job_id: &EnvelopeId,
        body: Option<Value>,
        bus: &Bus,
        pinned: u64,
    ) {
        if self.jobs.get(job_id).is_some_and(|j| j.state.notified) {
            return;
        }
        let effects = {
            let Some(job) = self.jobs.get_mut(job_id) else {
                return;
            };
            job.state.notified = true;
            match job.hooks.try_on_finished(body) {
                Ok(e) => e,
                Err(message) => {
                    self.pending_error.get_or_insert(EbaError::Other(message));
                    Vec::new()
                }
            }
        };
        self.apply_finished_effects(job_id, effects, bus, pinned);
    }
    /// finished job 只允许 tell/record/close；其它效果（含 finish）
    /// 一律拒绝并记错。
    fn apply_finished_effects(
        &mut self,
        job_id: &EnvelopeId,
        effects: Vec<Effect>,
        bus: &Bus,
        pinned: u64,
    ) {
        let mut allowed = Vec::new();
        for effect in effects {
            match effect {
                e @ (Effect::Tell { .. } | Effect::Record { .. } | Effect::Close { .. }) => {
                    allowed.push(e)
                }
                other => {
                    let kind = other.kind().to_string();
                    self.pending_error.get_or_insert_with(|| {
                        EbaError::Other(format!("unsupported onFinished effect: {}", kind))
                    });
                    // 遇到非法 kind 立即 throw：其后的合法效果不落地。
                    break;
                }
            }
        }
        self.apply_effects(job_id, allowed, bus, pinned);
    }
    /// 自回信主题且无在途租约时不合成 job_silent
    /// （避免自问自答回环）。
    fn suppress_silent(&self, job_id: &EnvelopeId) -> bool {
        let self_reply = self.jobs.get(job_id).is_some_and(|j| {
            matches!(
                &j.state.root.header.address,
                Address::Request { reply_to } if reply_to == &self.reply_to
            )
        });
        self_reply && !self.leases.holds(job_id)
    }
    /// commitTo 的终态判据：无在途租约、无 held、无延迟 post 时形成 Control → 回收。
    /// reply-only job 必须走这里，否则永久滞留在 `jobs` 中。
    fn finalize_if_idle(&mut self, job_id: &EnvelopeId, bus: &Bus, pinned: u64) {
        let info = self.jobs.get(job_id).map(|j| {
            (
                j.state.held,
                j.state.replied,
                j.state.reply_body.clone(),
                j.state.root.header.address.kind() == "request",
                j.state.root.clone(),
            )
        });
        let Some((held, replied, reply_body, is_request, root)) = info else {
            return;
        };
        let idle = !held
            && !self.pending_post.contains_key(job_id)
            && self.leases.count(job_id) == 0
            && !self.timers.iter().any(|t| &t.owner == job_id);
        if !idle {
            return;
        }
        // silence()：request 本轮收尾却未回信时合成 job_silent。
        let body = if replied {
            reply_body
        } else if is_request && !self.suppress_silent(job_id) {
            let body = err_payload("job_silent");
            let result = self.make_result(job_id, body.clone());
            self.settle_reply(bus, result);
            if let Some(job) = self.jobs.get_mut(job_id) {
                job.state.replied = true;
                job.state.reply_body = Some(body.clone());
            }
            let _ = root;
            Some(body)
        } else {
            None
        };
        self.notify_finished(job_id, body, bus, pinned);
        self.remove_job(job_id);
    }
    /// 回信发布失败必须上抛，不能静默丢结果。
    /// Mailbox::send 已按 统一包装为 DeliveryError，这里只记录一次。
    fn settle_reply(&mut self, bus: &Bus, result: Envelope) {
        if let Err(error) = self.mailbox.send(bus, result) {
            self.pending_error.get_or_insert(error);
        }
    }
    /// 非法 effect 被 catch 后回 job_failed 并回收 job，
    /// 同时把原始错误沿 advance 上抛（panic=abort 下不能 panic）。
    fn fail_effects(&mut self, job_id: &EnvelopeId, error: EbaError, bus: &Bus, pinned: u64) {
        self.pending_error.get_or_insert_with(|| error.clone());
        // 先触发一次 onFinished，再仅对未回信的 request 回
        // job_failed（带 message）。非 request / 已 replied 不得产生第二封回信。
        let detail = Value::Map(BTreeMap::from([(
            "message".into(),
            Value::Str(format!("{}", error)),
        )]));
        let body = crate::value::err_payload_detail("job_failed", detail);
        let (is_request, replied) = self
            .jobs
            .get(job_id)
            .map(|j| {
                (
                    matches!(j.state.root.header.address, Address::Request { .. }),
                    j.state.replied,
                )
            })
            .unwrap_or((false, false));
        self.notify_finished(job_id, Some(body.clone()), bus, pinned);
        if is_request && !replied {
            let result = self.make_result(job_id, body);
            self.settle_reply(bus, result);
        }
        self.remove_job(job_id);
    }
    /// reject：被拒的 request 必须收到错误回信，
    /// 否则调用方只能等超时。非 request 不发回信。
    fn reject_root(&mut self, bus: &Bus, root: &Envelope, reason: &str) {
        let topic = match &root.header.address {
            Address::Request { reply_to } => reply_to.clone(),
            _ => return,
        };
        match make_envelope(MakeEnvelope {
            topic,
            payload: err_payload(reason),
            from: self.actor_id.clone(),
            id_gen: self.id_gen.as_ref(),
            address: Address::Result {
                correlation_id: root.header.id.clone(),
            },
            cause: Some(root.header.id.clone()),
            sv: 1,
        }) {
            Ok(result) => self.settle_reply(bus, result),
            Err(error) => {
                self.pending_error.get_or_insert(error);
            }
        }
    }
    fn apply_effects(&mut self, job_id: &EnvelopeId, effects: Vec<Effect>, bus: &Bus, pinned: u64) {
        // 落地错误早退：先求值错误、再结算 job_failed、最后退出本轮（逐字保持求值顺序）。
        macro_rules! fail_return {
            ($error:expr) => {{
                let error = $error;
                self.fail_effects(job_id, error, bus, pinned);
                return;
            }};
        }
        // 109-133：落地阶段的权威校验——跨轮 held、
        // maxInflight（含在途租约）、ask/hold/settle 前置。可捕获错误，不 panic。
        if let Err(error) = self.validate_turn(job_id, &effects) {
            fail_return!(error);
        }
        // 147-153：在第一处 barrier/timer 处切分，
        // pre 立即落地，post 延迟到屏障/定时器兑现后重放（透传 mint 语义）。
        let checkpoint = effects
            .iter()
            .position(|e| matches!(e, Effect::Barrier { .. } | Effect::Timer { .. }));
        let (pre, post) = match checkpoint {
            Some(idx) => {
                let pre: Vec<Effect> = effects[..=idx].to_vec();
                let post = effects[idx + 1..].to_vec();
                // 校验只针对 pre（此时 barrier/timer 是 pre 的最后一支）。
                if let Err(error) = Self::validate_effects(&pre) {
                    fail_return!(error);
                }
                // checkpoint 前的终止 effect 会让 post 永不可达——抛错而非吞掉。
                if !post.is_empty() {
                    if let Some(terminal) = pre[..pre.len() - 1].iter().find(|e| {
                        matches!(
                            e,
                            Effect::Finish { .. }
                                | Effect::Settle { .. }
                                | Effect::Revoke { .. }
                                | Effect::Close { .. }
                        )
                    }) {
                        let error = EbaError::Other(format!(
                            "unsupported {} effect before a checkpoint with post effects",
                            terminal.kind()
                        ));
                        fail_return!(error);
                    }
                }
                (pre, post)
            }
            None => {
                if let Err(error) = Self::validate_effects(&effects) {
                    fail_return!(error);
                }
                (effects, Vec::new())
            }
        };
        if !post.is_empty() {
            self.pending_post.insert(job_id.clone(), post);
        }
        // 284-301 commitTo：control（finish）已形成时，
        // commit.requests 整批跳过提交。RS 若先 publish 再 finish，下游会收到
        // 一个永不会被结算的孤儿请求。预扫描本段是否存在 finish 决定门控。
        let finish_in_segment = pre.iter().any(|e| matches!(e, Effect::Finish { .. }));
        let mut finished_in_turn = false;
        for effect in pre {
            match effect {
                Effect::Ask { intent } => {
                    if finish_in_segment {
                        continue;
                    }
                    // 141-156：submit 直接 bus.publish，
                    // 不经过 mailbox 背压停车（ask 是出站请求，不走结果回信的 parking）。
                    // published==false 且无 deadline → due=ELAPSED；发布错误 → job_failed。
                    let delivered = match bus.publish(&intent.request) {
                        Ok(delivered) => delivered,
                        Err(error) => fail_return!(error),
                    };
                    let now = self.live_now(pinned);
                    let deadline = match intent.timeout_ms.or(self.timeout) {
                        Some(t) => match crate::time::safe_deadline(now.saturating_add(t)) {
                            Ok(d) => Some(d),
                            Err(e) => fail_return!(e),
                        },
                        None if !delivered => Some(now), // ELAPSED
                        None => None,
                    };
                    let lease = Lease::new(crate::job::lease::LeaseOptions {
                        deadline_ms: deadline,
                        budget_ms: None,
                    });
                    let req_id = intent.request.header.id.clone();
                    // rendezvous.Record 存 replyTo 作为期望 topic，cause 为请求 id
                    // （RS make_result 的回信 cause=Some(job_id) 与此对应）。
                    let reply_to = match &intent.request.header.address {
                        Address::Request { reply_to } => reply_to.clone(),
                        _ => "sys.reply".to_string(),
                    };
                    if let Err(e) = self.leases.submit(
                        req_id.clone(),
                        lease,
                        job_id.clone(),
                        intent.stage.clone(),
                        deadline,
                        reply_to,
                        Some(req_id),
                    ) {
                        fail_return!(e);
                    }
                }
                Effect::Reply { body } => {
                    // 字面量 Effect 绕过 builder：落地口兜底校验（applyEffects 唯一权威）。
                    if body != Value::Null && !crate::value::has_result_shape(&body) {
                        fail_return!(EbaError::Other("invalid result body".into()));
                    }
                    // commitTo：reply 且无在途租约/等待时形成 Control → seal+notify+drop。
                    // 旧实现只发回信、不回收 job，导致 reply-only job 永久滞留、has_work 恒真。
                    if let Some(job) = self.jobs.get_mut(job_id) {
                        job.state.replied = true;
                        job.state.reply_body = Some(body.clone());
                    }
                    let result = self.make_result(job_id, body);
                    self.settle_reply(bus, result);
                    self.finalize_if_idle(job_id, bus, pinned);
                }
                Effect::Finish { body } => {
                    // 字面量 Effect 绕过 builder：落地口兜底校验（finishControl）。
                    if body != Value::Null && !crate::value::has_result_shape(&body) {
                        fail_return!(EbaError::Other("invalid result body".into()));
                    }
                    // finishControl：control 已存在时重复 finish 是 no-op，不能二次回信。
                    if finished_in_turn {
                        continue;
                    }
                    finished_in_turn = true;
                    // deliverEnding：先 markReplied + 发真正回信，再 settleEnding → onFinished。
                    // 顺序颠倒会让 onFinished 的 close 误判为静默 job 合成 job_silent，双回信。
                    if let Some(job) = self.jobs.get_mut(job_id) {
                        job.state.replied = true;
                        job.state.reply_body = Some(if body == Value::Null {
                            err_payload("job_silent")
                        } else {
                            body.clone()
                        });
                    }
                    // make_result 需要 root 的 reply_to，因此必须在删除 job 之前构造。
                    // 先发真正回信再触发 onFinished，避免清理效果的 close 误判静默并双回信。
                    let result = self.make_result(job_id, body.clone());
                    self.settle_reply(bus, result);
                    // settleEnding → notify：正常终结触发一次 onFinished。
                    self.notify_finished(job_id, Some(body), bus, pinned);
                    // 根请求已终结：立刻回收 job。否则 Finished 的 job 会永久留在
                    // `jobs` 里，`has_work()` 恒为真、`is_quiescent()` 永不成立，
                    // 每次 ask 都泄漏一个 Job 及其 state（实测 ~2.3 KB/次，线性不收敛）。
                    self.remove_job(job_id);
                }
                Effect::Hold { lease } => {
                    // 117 assertLeaseIssued：手造未绑定 lease 不得安装。
                    if !lease.is_bound_to(job_id.as_str()) {
                        fail_return!(EbaError::Other("lease was not issued for this job".into()));
                    }
                    // 119 markHeld(state, true)：
                    // 不置位会让 held job 被 revoke/finalize_if_idle 误回收。
                    if let Some(job) = self.jobs.get_mut(job_id) {
                        job.state.held = true;
                    }
                    self.held.insert(job_id.clone(), lease);
                }
                Effect::Settle { lease, body } => {
                    if !lease.is_bound_to(job_id.as_str()) {
                        fail_return!(EbaError::Other("lease was not issued for this job".into()));
                    }
                    let _ = lease.settle(body);
                }
                Effect::Revoke { lease, error } => {
                    if !lease.is_bound_to(job_id.as_str()) {
                        fail_return!(EbaError::Other("lease was not issued for this job".into()));
                    }
                    let _ = lease.revoke(error.unwrap_or_else(|| "cancelled".into()));
                }
                Effect::Tell { topic, payload } => {
                    // tell 失败是可捕获 Error → job_failed，不静默记错也不 panic。
                    let built = make_envelope(MakeEnvelope {
                        topic,
                        payload,
                        from: self.actor_id.clone(),
                        id_gen: self.id_gen.as_ref(),
                        address: Address::Event,
                        cause: None,
                        sv: 1,
                    });
                    let sent = built.and_then(|env| bus.tell(&env).map(|_| ()));
                    if let Err(error) = sent {
                        fail_return!(EbaError::Delivery(format!("tell effect failed: {}", error)));
                    }
                }
                Effect::Record { topic, payload } => {
                    let built = make_envelope(MakeEnvelope {
                        topic,
                        payload,
                        from: self.actor_id.clone(),
                        id_gen: self.id_gen.as_ref(),
                        address: Address::Event,
                        cause: None,
                        sv: 1,
                    });
                    let error = match built {
                        Err(e) => Some(e),
                        Ok(env) => {
                            // 50-65：ring 满则 drain 后重试，仍满才拒；
                            // 序号耗尽（SeqExhausted）不可通过 drain 恢复，直接上抛。
                            match self.records.record_checked(&env, self.live_now(pinned)) {
                                Err(e) => Some(e),
                                Ok(Some(_)) => None,
                                Ok(None) => match self.records.drain() {
                                    Err(e) => Some(e),
                                    Ok(_) => match self
                                        .records
                                        .record_checked(&env, self.live_now(pinned))
                                    {
                                        Err(e) => Some(e),
                                        Ok(Some(_)) => None,
                                        Ok(None) => {
                                            Some(EbaError::Backpressure("record ring full".into()))
                                        }
                                    },
                                },
                            }
                        }
                    };
                    if let Some(error) = error {
                        fail_return!(error);
                    }
                }
                Effect::Barrier { up_to } => {
                    let _ = self.records.settle();
                    // barrier 等待 durable 水位；记录当前目标，settle_records 命中后重放 post。
                    let target = up_to.unwrap_or_else(|| self.records.last_seq());
                    // 336-342：目标超出已记录 seq 是普通 Error（非 panic）。
                    if target > self.records.last_seq() {
                        let error = EbaError::Other(format!(
                            "barrier target beyond recorded seq: {} > {}",
                            target,
                            self.records.last_seq()
                        ));
                        fail_return!(error);
                    }
                    // 已达标与否都登记 watermark，下一轮由 settle_records 触发重放。
                    self.barrier_watermark.insert(job_id.clone(), target);
                }
                Effect::Timer { deadline_ms } => {
                    // 非法 deadline 记错并终止当轮（普通 Error 文本）。
                    let d = match safe_deadline(deadline_ms) {
                        Ok(d) => d,
                        Err(e) => fail_return!(e),
                    };
                    self.timers.push(TimerEntry {
                        owner: job_id.clone(),
                        deadline: d,
                    });
                }
                Effect::Close { .. } => {
                    self.close_requested = true;
                    // ctx.close() 也必须结算静默 job 的调用方。
                    let silent = self.jobs.get(job_id).is_some_and(|j| {
                        !j.state.replied && j.state.root.header.address.kind() == "request"
                    });
                    if silent && !self.suppress_silent(job_id) {
                        let body = err_payload("job_silent");
                        let result = self.make_result(job_id, body.clone());
                        self.settle_reply(bus, result);
                        if let Some(job) = self.jobs.get_mut(job_id) {
                            job.state.replied = true;
                            job.state.reply_body = Some(body);
                        }
                    }
                }
            }
        }
    }
    fn make_result(&self, job_id: &EnvelopeId, body: Value) -> Envelope {
        // 回信 topic 取 root 请求的 reply_to（即发问宿主的回信主题），
        // 这样嵌套 ask 的回信能回到真正的发问 host，而非固定 front。
        let topic = self
            .jobs
            .get(job_id)
            .and_then(|job| match &job.state.root.header.address {
                Address::Request { reply_to } => Some(reply_to.clone()),
                _ => None,
            })
            .unwrap_or_else(|| "sys.reply".to_string());
        make_envelope(MakeEnvelope {
            topic,
            payload: body,
            from: self.actor_id.clone(),
            id_gen: self.id_gen.as_ref(),
            address: Address::Result {
                correlation_id: job_id.clone(),
            },
            cause: Some(job_id.clone()),
            sv: 1,
        })
        .expect("make result envelope")
    }
    /// barrier 水位达标后重放 post effect 段。
    /// 返回是否有重放发生。
    pub fn replay_ready_barriers(&mut self, bus: &Bus, pinned: u64) -> bool {
        let ready: Vec<EnvelopeId> = self
            .barrier_watermark
            .iter()
            .filter(|(_, target)| self.records.durable() >= **target)
            .map(|(job, _)| job.clone())
            .collect();
        let mut progressed = false;
        for job in ready {
            self.barrier_watermark.remove(&job);
            if let Some(post) = self.pending_post.remove(&job) {
                self.apply_effects(&job, post, bus, pinned);
                // 延迟段落地后仍走 commitTo → seal/drop。
                self.finalize_if_idle(&job, bus, pinned);
                progressed = true;
            } else {
                // barrier 兑现后即使无 post 也要跑一轮
                // onResume（→ commitTo → onFinished → drop）。缺此回退会让
                // 纯 barrier job 永久滞留、has_work() 恒真。
                let (effects, hook_error) = {
                    let Some(job_mut) = self.jobs.get_mut(&job) else {
                        continue;
                    };
                    let reply_to = self.reply_to.clone();
                    run_hook(
                        job_mut,
                        self.actor_id.clone(),
                        reply_to,
                        self.id_gen.as_ref(),
                        |hooks, ctx| {
                            let returned = hooks.try_on_resume(ctx)?;
                            Ok(ctx.collect(returned))
                        },
                    )
                };
                self.apply_effects(&job, effects, bus, pinned);
                self.finalize_if_idle(&job, bus, pinned);
                if let Some(error) = hook_error {
                    self.pending_error
                        .get_or_insert(crate::errors::EbaError::Other(error));
                }
                progressed = true;
            }
        }
        progressed
    }
    /// timer 到点后重放其 post 段；无 post 则回退到 onResume。
    pub fn replay_timer_post(&mut self, owner: &EnvelopeId, bus: &Bus, pinned: u64) -> bool {
        if let Some(post) = self.pending_post.remove(owner) {
            self.apply_effects(owner, post, bus, pinned);
            // timer 延迟段落地后仍要 commitTo 收尾。
            self.finalize_if_idle(owner, bus, pinned);
            true
        } else {
            false
        }
    }
    pub fn settle_records(&mut self) -> Result<bool> {
        self.records.settle()
    }
    /// 一轮 pump：有限预算 + 连续 4 次无进展退出。
    pub fn pump(&mut self, bus: &Bus, pinned: u64) -> bool {
        // 进入即消费唤醒旗标：本轮会把这些相都跑一遍，跑完若仍有信会重新 notify。
        // 若不清除，`wants_pump` 会恒为真，退化成每轮全扫（即轮询）。
        self.signal
            .clear(crate::notify::READ | crate::notify::TIMER | crate::notify::WORK);
        // 每轮重算暂停门控与关闭租约到期。
        // holding（有在途租约）且入站队列满时暂停新工作，只收 result；
        // inbox 已关闭时把无 deadline 的 open 租约挂 ELAPSED=0，避免调用方永久挂起。
        let holding = !self.held.is_empty() || self.leases.size() > 0;
        if holding && self.queue.len() >= self.queue_limit {
            self.sub.pause_work();
        } else {
            self.sub.resume_work();
        }
        if matches!(self.state, HostState::Running | HostState::Draining) && self.sub.is_closed() {
            self.leases.elapse_open();
        }
        let mut progressed = false;
        let mut idle = 0;
        let mut budget = self.work_budget;
        while budget > 0 && idle < 4 {
            let phase = self.phase;
            self.phase = (self.phase + 1) % 4;
            // 四相严格分离（READ/TIMER/WORK/DELIVERY）。
            // 旧实现把 timer 合进 phase 0 又在 phase 1 重复调用，导致 timer effect
            // 提前/重复落地、read 与 timer 相对顺序错误。
            let advanced = match phase {
                0 => self.phase_read(bus, pinned),
                1 => self.phase_timer(bus, pinned),
                2 => self.phase_work(bus, pinned),
                3 => self.phase_delivery(bus),
                _ => false,
            };
            if advanced {
                progressed = true;
                idle = 0;
                budget -= 1;
            } else {
                idle += 1;
            }
        }
        progressed
    }
    /// 入站 admit：直接构造 Wait job（Call 不经 Value 载荷）。
    pub fn admit_call(&mut self, call: Call, bus: &Bus, pinned: u64) {
        let root = make_envelope(MakeEnvelope {
            topic: "sys.admit".into(),
            payload: Value::Null,
            from: call.from.clone(),
            id_gen: self.id_gen.as_ref(),
            address: Address::Request {
                reply_to: "sys.reply".into(),
            },
            cause: None,
            sv: 1,
        })
        .expect("admit root");
        let root_id = root.header.id.clone();
        let hooks: Box<dyn JobHooks> = Box::new(WaitHooks::new(call));
        let mut job = Job::new(root, hooks, 100);
        job.state.transition(Lifecycle::Active);
        let reply_to = self.reply_to.clone();
        let (effects, hook_error) = {
            run_hook(
                &mut job,
                self.actor_id.clone(),
                reply_to,
                self.id_gen.as_ref(),
                |hooks, ctx| {
                    let returned = hooks.try_begin(ctx)?;
                    Ok(ctx.collect(returned))
                },
            )
        };
        self.jobs.insert(root_id.clone(), job);
        self.apply_effects(&root_id, effects, bus, pinned);
        if hook_error.is_some() {
            self.pending_error = hook_error.map(crate::errors::EbaError::Other);
        }
    }
}
