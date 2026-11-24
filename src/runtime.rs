//! 运行时：Runtime / Pending / Drive / lifecycle。
//! ask → advance → Pending.ready 兑现。
use crate::envelope::{
    make_envelope, Address, Completion, Envelope, EnvelopeId, IdGen, MakeEnvelope,
};
use crate::errors::{EbaError, Result};
use crate::job::core::{Core, HostState};
use crate::job::job::{Call, CompletionSlot, FinishJob, JobHooks, JobHost};
use crate::mail::{Bus, Inbox};
use crate::notify::{Agenda, Signal};
use crate::pattern::parse_pattern;
use crate::prelude::*;
use crate::record::RecordLog;
use crate::time::{saturating_now, Clock, Schedule};
use crate::value::{err_payload, Value};
#[derive(Clone, Debug)]
pub struct HostStats {
    pub active: usize,
    pub held: usize,
    pub queued: usize,
    pub pending: usize,
    pub rejected: usize,
    /// 待传播取消数 / 邮箱积压数。
    pub pending_cancels: usize,
    pub mailbox: usize,
}
#[derive(Clone, Debug)]
pub struct RuntimeStats {
    pub hosts: usize,
    pub active: usize,
    pub held: usize,
    pub queued: usize,
    pub pending: usize,
    pub rejected: usize,
    pub pending_cancels: usize,
    pub mailbox: usize,
    pub hosts_detail: BTreeMap<String, HostStats>,
}
struct Host {
    core: Core,
}
/// 回执：ready() 轮询兑现。
pub struct Pending {
    slot: CompletionSlot,
}
impl Pending {
    pub fn ready(&self) -> Option<Completion> {
        self.slot.take()
    }
    pub fn peek(&self) -> Option<Completion> {
        self.slot.peek()
    }
    pub fn is_ready(&self) -> bool {
        self.slot.peek().is_some()
    }
}
/// 单写者运行时。advance(&mut self) 是唯一推进入口（编译期保证）。
pub struct Runtime {
    bus: Bus,
    clock: Rc<dyn Clock>,
    id_gen: Rc<dyn IdGen>,
    hosts: BTreeMap<String, Host>,
    front: Core,
    agenda: Agenda,
    state: HostState,
    host_id_seq: u64,
    claim: Option<crate::mail::ClaimToken>,
    /// 跨轮 delta 时钟，供 charge_leases 使用（pinTurn.lastNowMs）。
    last_now_ms: Option<u64>,
    /// 退役 host 时未能结算的信封，由 close() 一并返回调用方。
    abandoned: Vec<Envelope>,
    /// closeAsync 失败时 stashRoots：留下的 roots 由下次 close 返回。
    stash: Vec<Envelope>,
    /// 按调用先后记录有序事件，
    /// advance 开头依序 drain 到 Agenda（Rust 无 WeakRef<Agenda>，用共享队列代替）。
    /// 单一队列保证同轮内 clear→notify 的交错顺序不被两个独立队列的固定 drain 顺序吞掉。
    signal_events: Rc<RefCell<Vec<(u64, bool)>>>,
    /// Signal 重绑/detach 时对旧 owner 的 unbind 队列（actor 名）。
    unbind_queue: Rc<RefCell<Vec<String>>>,
    /// #front() 惰性注册：首次 ask/askWith 时把 front caller 加入 hosts 计数。
    front_activated: bool,
}
impl Runtime {
    pub fn new(bus: Bus, clock: Rc<dyn Clock>, id_gen: Rc<dyn IdGen>) -> Result<Self> {
        let owner = 1u64;
        let claim = bus.claim(owner)?;
        let mut agenda = Agenda::new();
        // front host：持有 Wait jobs 与 Call 完成槽，订阅 sys.reply.**。
        let signal_events: Rc<RefCell<Vec<(u64, bool)>>> = Rc::new(RefCell::new(Vec::new()));
        let unbind_queue: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
        let front_inbox = Inbox::new(256);
        let front_signal = Rc::new(Signal::new());
        let front_records = RecordLog::new(Box::new(crate::record::MemorySink::new()), 64, 1);
        let mut front = Core::new(
            "sys.ask",
            front_inbox.clone(),
            Box::new(|_| Box::new(FinishJob(Value::Null)) as Box<dyn JobHooks>),
            front_signal.clone(),
            front_records,
            id_gen.clone(),
            0,
        );
        front.reply_to = "sys.reply".into();
        front.attach_clock(clock.clone());
        front_inbox.bind_signal(front_signal.clone());
        bus.subscribe("sys.ask", parse_pattern("sys.reply.**")?, front_inbox);
        front_signal.bind(owner, "sys.ask");
        agenda.bind_signal("sys.ask", front_signal.clone());
        if let Some(aid) = agenda.id_of("sys.ask") {
            let sq = signal_events.clone();
            front_signal.set_wake(Rc::new(move || sq.borrow_mut().push((aid, true))));
            let sq = signal_events.clone();
            front_signal.set_unwake(Rc::new(move || sq.borrow_mut().push((aid, false))));
        }
        {
            let ubq = unbind_queue.clone();
            front_signal.set_unbind(Rc::new(move || ubq.borrow_mut().push("sys.ask".into())));
        }
        agenda.update("sys.ask", None, false, false);
        Ok(Self {
            bus,
            clock,
            id_gen,
            hosts: BTreeMap::new(),
            front,
            agenda,
            state: HostState::Running,
            host_id_seq: 1,
            claim: Some(claim),
            last_now_ms: None,
            abandoned: Vec::new(),
            stash: Vec::new(),
            signal_events,
            unbind_queue,
            front_activated: false,
        })
    }
    pub fn add(&mut self, host: JobHost) -> Result<()> {
        self.add_with_options(host, crate::job::HostOptions::default())
    }
    /// add_with_options：按 `HostOptions` 调整每个 host 的有界上限。
    pub fn add_with_options(
        &mut self,
        host: JobHost,
        options: crate::job::HostOptions,
    ) -> Result<()> {
        if self.state != HostState::Running {
            return Err(EbaError::Closed("closed or reentrant runtime".into()));
        }
        let actor = host.actor_id.clone();
        if self.hosts.contains_key(&actor) {
            return Err(EbaError::Other("duplicate host actor".into()));
        }
        let signal = Rc::new(Signal::new());
        let id = self.host_id_seq;
        self.host_id_seq += 1;
        signal.bind(id, &actor);
        let records = RecordLog::new(Box::new(crate::record::MemorySink::new()), 64, 1);
        let mut core = Core::new(
            actor.clone(),
            host.inbox.clone(),
            host.make_job,
            signal.clone(),
            records,
            self.id_gen.clone(),
            id,
        );
        // 应用可调上限。
        core.work_budget = options.work_budget;
        core.queue_limit = options.queue_limit;
        core.active_limit = options.active_limit;
        core.timeout = options.request_timeout_ms;
        // 每个 host 有独立回信主题，回信才能回到真正的发问 host（嵌套 ask）。
        // topic 形如 reply.<actor>.<jobId>，订阅 reply.<actor>.**。
        let reply_topic = format!("reply.{}", actor);
        core.reply_to = reply_topic.clone();
        core.attach_clock(self.clock.clone());
        host.inbox.bind_signal(signal.clone());
        for p in &host.patterns {
            self.bus.subscribe(&actor, p.clone(), host.inbox.clone());
        }
        // 订阅本 host 的回信主题（用独立 actor 键避免覆盖业务订阅）。
        let reply_pattern = format!("{}.**", reply_topic);
        self.bus.subscribe(
            &format!("{}#reply", actor),
            parse_pattern(&reply_pattern)?,
            host.inbox.clone(),
        );
        self.agenda.bind_signal(&actor, signal.clone());
        // 回调只写 agenda 的稳定 u64 id，不克隆 actor 名——这是推进热路径上
        // 最大的一笔固定开销（每次 notify 一次 String 分配）。
        if let Some(aid) = self.agenda.id_of(&actor) {
            let sq = self.signal_events.clone();
            signal.set_wake(Rc::new(move || sq.borrow_mut().push((aid, true))));
            let sq = self.signal_events.clone();
            signal.set_unwake(Rc::new(move || sq.borrow_mut().push((aid, false))));
        }
        {
            let ubq = self.unbind_queue.clone();
            let actor_name = actor.clone();
            signal.set_unbind(Rc::new(move || ubq.borrow_mut().push(actor_name.clone())));
        }
        self.agenda.update(&actor, None, false, false);
        self.hosts.insert(actor, Host { core });
        Ok(())
    }
    /// ask：创建 Call + Wait job，返回 Pending 回执。
    pub fn ask(
        &mut self,
        topic: impl Into<String>,
        payload: Value,
        from: impl Into<String>,
        timeout_ms: Option<u64>,
    ) -> Pending {
        // front 处于 draining/closing/closed 时拒绝新 ask，
        // 否则 Pending 永远无法兑现（advance 以 Closed 拒绝推进）。
        assert!(
            self.state == HostState::Running,
            "cannot ask on a closing/closed runtime"
        );
        self.admit_front();
        // #front()：首次 ask 惰性注册 front caller（影响 stats().hosts）。
        self.front_activated = true;
        let (pending, call) = self.make_call(from, topic, payload, timeout_ms);
        let pinned = saturating_now(self.clock.as_ref());
        self.front.admit_call(call, &self.bus, pinned);
        self.sync_front();
        pending
    }
    /// `ask(topic, payload, from = "caller", timeoutMs?)` 的默认参数形态。
    pub fn ask_default(&mut self, topic: impl Into<String>, payload: Value) -> Pending {
        self.ask(topic, payload, "caller", None)
    }
    /// askWith：结算时把 Completion 交给 pay，恰好一次。
    pub fn ask_with(
        &mut self,
        topic: impl Into<String>,
        payload: Value,
        from: impl Into<String>,
        timeout_ms: Option<u64>,
        pay: impl FnOnce(Completion) + 'static,
    ) {
        assert!(
            self.state == HostState::Running,
            "cannot ask on a closing/closed runtime"
        );
        self.admit_front();
        self.front_activated = true;
        let (_pending, call) = self.make_call(from, topic, payload, timeout_ms);
        call.on_pay(Box::new(pay));
        let pinned = saturating_now(self.clock.as_ref());
        self.front.admit_call(call, &self.bus, pinned);
        self.sync_front();
    }
    /// Caller.admit：front 过载时同步拒绝（active_full / mailbox full）。
    /// RS 的 ask 返回 Pending 而非 Result，按 B 类"panic 代 throw"表达拒绝。
    fn admit_front(&self) {
        let crowded = self.front.jobs.len() + self.front.queue.len() >= self.front.active_limit;
        assert!(!crowded, "active_full");
        assert!(self.front.sub.can_enqueue("request"), "target mailbox full");
    }
    fn make_call(
        &self,
        from: impl Into<String>,
        topic: impl Into<String>,
        payload: Value,
        timeout_ms: Option<u64>,
    ) -> (Pending, Call) {
        let slot = CompletionSlot::new();
        let call = Call::new_from(from, topic, payload, timeout_ms, slot.clone());
        (Pending { slot }, call)
    }
    /// tell：fire-and-forget event。
    pub fn tell(&mut self, topic: impl Into<String>, payload: Value, from: impl Into<String>) {
        // 关闭后 tell 抛错，不静默丢弃。
        assert!(
            self.state != HostState::Closing && self.state != HostState::Closed,
            "runtime is closed"
        );
        let env = make_envelope(MakeEnvelope {
            topic: topic.into(),
            payload,
            from: from.into(),
            id_gen: self.id_gen.as_ref(),
            address: Address::Event,
            cause: None,
            sv: 1,
        })
        .expect("tell envelope");
        self.bus.tell(&env).expect("tell envelope");
    }
    /// 单写者推进一小步；返回是否有进展。
    /// 宿主默认请求超时。
    /// 每一相都用**显式的字段分离借用**（`hosts` / `bus` / `agenda` 各自独立），
    /// 因此不需要把宿主名克隆进 `Vec<String>`。空转一轮不再分配任何临时容器，
    /// 也不再逐宿主 `to_string()`——这是推进热路径上最大的一笔固定开销。
    pub fn advance(&mut self) -> Result<bool> {
        if self.state == HostState::Closed {
            return Err(EbaError::Reentrant("closed runtime".into()));
        }
        // 重试 pending 投递（满则保留到下一轮）。
        self.bus.flush_pending_deliveries();
        // drain 同步唤醒队列到 Agenda。
        {
            // 依调用先后 drain：同轮 clear→notify 交错时最终状态与 同步执行一致。
            let events = core::mem::take(&mut *self.signal_events.borrow_mut());
            for (aid, wake) in events {
                if wake {
                    self.agenda.enqueue_id(aid);
                } else {
                    self.agenda.unqueue_id(aid);
                }
            }
            let unbound = core::mem::take(&mut *self.unbind_queue.borrow_mut());
            for actor in unbound {
                self.agenda.unbind_signal(&actor);
            }
        }
        // wake 回调记录的 fault 包装成 WakeError 抛出。
        if let Some(message) = self.agenda.take_fault() {
            return Err(EbaError::Wake(message));
        }
        let pinned = saturating_now(self.clock.as_ref());
        // 跨轮 delta（pinTurn.lastNowMs），首轮为 0，严格单调递增。
        let delta = self
            .last_now_ms
            .map(|last| pinned.saturating_sub(last))
            .unwrap_or(0);
        self.last_now_ms = Some(pinned);
        let mut progressed = false;
        // 前置：每 host 扣租约（真实 delta）、到期 held、结算记录。
        for host in self.hosts.values_mut() {
            host.core.charge_leases(delta);
            host.core.expire_held(pinned);
            if host.core.settle_records()? {
                progressed = true;
            }
        }
        self.front.charge_leases(delta);
        self.front.expire_held(pinned);
        if self.front.settle_records()? {
            progressed = true;
        }
        // barrier 水位达标 → 重放 post 段（hosts 与 bus 分离借用）。
        {
            let hosts = &mut self.hosts;
            let bus = &self.bus;
            for host in hosts.values_mut() {
                if host.core.replay_ready_barriers(bus, pinned) {
                    progressed = true;
                }
            }
        }
        if self.front.replay_ready_barriers(&self.bus, pinned) {
            progressed = true;
        }
        self.agenda.due(pinned);
        // 所有 host（含 front）的租约都到期并在各自 core 内恢复 owner job，
        // 不再只 front。
        if self.front.expire_and_resume(&self.bus, pinned) {
            progressed = true;
        }
        {
            let hosts = &mut self.hosts;
            let bus = &self.bus;
            for host in hosts.values_mut() {
                if host.core.expire_and_resume(bus, pinned) {
                    progressed = true;
                }
            }
        }
        // 收集所有 host 到期/被撤的 requestId，跨 host 传播 revoke，
        // 让下游 worker 的 job 也能 onFinished，宿主收敛到 quiescent。有界 256 轮防环。
        if self.propagate_cancellations(pinned)? {
            progressed = true;
        }
        // 事件驱动推进：只 pump 被唤醒的宿主。
        // 到期与撤销的 requestId 跨 host 集中传播。
        // 入队时 `Inbox::enqueue` 已 notify(READ|WORK)，因此 `signal.has(READ)`
        // 就是"这个宿主有新信或新工作"的权威判据——不需要逐轮询问全部宿主。
        // 空闲轮只做一次按位与，13 个宿主全部跳过。
        if self.front.wants_pump() {
            let moved = self.front.pump(&self.bus, pinned);
            progressed |= moved;
            sync_front_parts(&mut self.agenda, &self.front);
        }
        {
            let hosts = &mut self.hosts;
            let bus = &self.bus;
            let agenda = &mut self.agenda;
            for (name, host) in hosts.iter_mut() {
                if !host.core.wants_pump() {
                    continue;
                }
                let moved = host.core.pump(bus, pinned);
                progressed |= moved;
                sync_host_parts(agenda, name, &host.core);
            }
        }
        // hook 的结算先落地，错误再沿 advance 抛出。
        if let Some(error) = self.front.pending_error.take() {
            return Err(error);
        }
        for host in self.hosts.values_mut() {
            if let Some(error) = host.core.pending_error.take() {
                return Err(error);
            }
        }
        {
            let hosts = &mut self.hosts;
            let bus = &self.bus;
            let agenda = &mut self.agenda;
            for (name, host) in hosts.iter_mut() {
                // 只有持有租约的宿主才可能 resolve；无租约直接跳过。
                if host.core.held.is_empty() {
                    continue;
                }
                if host.core.resolve_held(bus, pinned) {
                    progressed = true;
                    sync_host_parts(agenda, name, &host.core);
                }
            }
        }
        if self.front.resolve_held(&self.bus, pinned) {
            progressed = true;
            sync_front_parts(&mut self.agenda, &self.front);
        }
        // DELIVERY 相 bury 的信件必须结算 delivery_failed 释放等待者。
        if self.settle_all_buried(pinned) {
            progressed = true;
        }
        // revoke 在第二轮又可能产生新的 cancelled（级联），再收敛一次。
        if self.propagate_cancellations(pinned)? {
            progressed = true;
        }
        // 关停宿主：只有真正请求关闭的宿主才进入这个列表，正常推进时为空。
        let closing: Vec<String> = self
            .hosts
            .iter()
            .filter(|(_, h)| h.core.close_requested)
            .map(|(a, _)| a.clone())
            .collect();
        for actor in closing {
            self.retire_host(&actor);
            progressed = true;
        }
        Ok(progressed)
    }
    /// 把所有 core 到期/撤销的 requestId 集中起来，广播给每个 core
    /// 的 revoke，让下游 worker job 也 onFinished、宿主收敛到 quiescent。
    /// 有界 256 轮：revoke 可能级联产生新的 cancelled，循环到收敛为止。
    fn propagate_cancellations(&mut self, pinned: u64) -> Result<bool> {
        // 空转快路径：没有任何 core 持有取消意图时，连 BTreeSet/Vec 都不构造。
        // 原实现每轮 advance 都先 `BTreeSet::new() + Vec::new()`，即使本轮
        // cancelled 恒空（第一轮就 break）——这是纯粹的固定分配开销。
        if !self.front.has_cancellations()
            && !self.hosts.values().any(|h| h.core.has_cancellations())
        {
            return Ok(false);
        }
        let mut any = false;
        let mut seen: BTreeSet<EnvelopeId> = BTreeSet::new();
        for _round in 0..256 {
            let mut cancelled: Vec<EnvelopeId> = self.front.take_cancelled();
            cancelled.extend(self.front.take_pending_cancels());
            {
                let hosts = &mut self.hosts;
                for host in hosts.values_mut() {
                    cancelled.extend(host.core.take_cancelled());
                    cancelled.extend(host.core.take_pending_cancels());
                }
            }
            if cancelled.is_empty() {
                break;
            }
            // seen 防转：过滤已处理过的 id，无新增时立即停。
            cancelled.retain(|id| seen.insert(id.clone()));
            if cancelled.is_empty() {
                break;
            }
            any = true;
            let bus = &self.bus;
            self.front.revoke(&cancelled, bus, pinned);
            let hosts = &mut self.hosts;
            for host in hosts.values_mut() {
                host.core.revoke(&cancelled, bus, pinned);
            }
        }
        Ok(any)
    }
    ///  bury 后结算 delivery_failed，释放等待者，防止调用方永久挂起。
    fn settle_all_buried(&mut self, pinned: u64) -> bool {
        let (mut progressed, unhandled_front) = self.front.settle_buried(&self.bus, pinned);
        let mut unhandled = unhandled_front;
        {
            let hosts = &mut self.hosts;
            let bus = &self.bus;
            for host in hosts.values_mut() {
                let (p, u) = host.core.settle_buried(bus, pinned);
                progressed |= p;
                unhandled.extend(u);
            }
        }
        // answerSpent: 本地无持有者时把合成失败信封回投 bus。
        for letter in unhandled {
            let failure = make_envelope(MakeEnvelope {
                topic: letter.header.topic.clone(),
                payload: err_payload("delivery_failed"),
                from: letter.header.from.clone(),
                id_gen: self.id_gen.as_ref(),
                address: Address::Result {
                    correlation_id: letter.correlation_id().cloned().unwrap_or_default(),
                },
                cause: letter.header.cause.clone(),
                sv: 1,
            })
            .expect("delivery_failed envelope");
            // 走 deliver 而非 publish，满时 park 等下一轮重投，
            // 避免合成 delivery_failed 被静默丢弃。
            let home = self.agenda.id_of("sys.ask").unwrap_or(0);
            let _ = self.bus.deliver(&failure, home);
        }
        progressed
    }
    fn sync_front(&mut self) {
        sync_front_parts(&mut self.agenda, &self.front);
    }
    fn retire_host(&mut self, actor: &str) {
        let pinned = saturating_now(self.clock.as_ref());
        let settled = {
            let bus = &self.bus;
            match self.hosts.get_mut(actor) {
                Some(host) => host
                    .core
                    .close_and_collect(bus, pinned)
                    .expect("close host"),
                None => return,
            }
        };
        // 只有记录成功落盘才真正回收 host；失败时 host 保留供下次重试。
        self.hosts.remove(actor);
        self.abandoned.extend(settled);
        self.bus.unsubscribe(actor);
        self.agenda.remove(actor);
    }
    pub fn schedule(&self) -> Schedule {
        self.agenda.schedule(saturating_now(self.clock.as_ref()))
    }
    pub fn stats(&self) -> RuntimeStats {
        let mut detail = BTreeMap::new();
        let mut t = HostStats {
            active: 0,
            held: 0,
            queued: 0,
            pending: 0,
            rejected: 0,
            pending_cancels: 0,
            mailbox: 0,
        };
        // front caller 首次 ask 时才惰性注册（#front()），
        // 此前既不计入 hosts 也不出现在 hostsDetail。
        if self.front_activated {
            let front = &self.front;
            let s = HostStats {
                active: front.jobs.len(),
                held: front.held.len(),
                queued: front.queue.len(),
                pending: front.leases.size(),
                rejected: front.rejected_count,
                pending_cancels: front.pending_cancels_len(),
                mailbox: front.mailbox_len(),
            };
            t.active += s.active;
            t.held += s.held;
            t.queued += s.queued;
            t.pending += s.pending;
            t.rejected += s.rejected;
            t.pending_cancels += s.pending_cancels;
            t.mailbox += s.mailbox;
            detail.insert("sys.ask".into(), s);
        }
        for (actor, host) in &self.hosts {
            let s = HostStats {
                active: host.core.jobs.len(),
                held: host.core.held.len(),
                queued: host.core.queue.len(),
                pending: host.core.leases.size(),
                rejected: host.core.rejected_count,
                pending_cancels: host.core.pending_cancels_len(),
                mailbox: host.core.mailbox_len(),
            };
            t.active += s.active;
            t.held += s.held;
            t.queued += s.queued;
            t.pending += s.pending;
            t.rejected += s.rejected;
            t.pending_cancels += s.pending_cancels;
            t.mailbox += s.mailbox;
            detail.insert(actor.clone(), s);
        }
        RuntimeStats {
            // 79 + #front()：front caller 首次 ask 时经 add() 注册，
            // 因此计入 hosts 断言 1 host + ask = 2）。
            hosts: self.hosts.len() + usize::from(self.front_activated),
            active: t.active,
            held: t.held,
            queued: t.queued,
            pending: t.pending,
            rejected: t.rejected,
            pending_cancels: t.pending_cancels,
            mailbox: t.mailbox,
            hosts_detail: detail,
        }
    }
    /// 暴露 Agenda 供宿主设置/消费 wake fault。
    pub fn agenda(&self) -> &Agenda {
        &self.agenda
    }
    pub fn state(&self) -> HostState {
        self.state
    }
    pub fn is_quiescent(&self) -> bool {
        self.agenda.ready_size() == 0
            && self.front.jobs.is_empty()
            && self.hosts.values().all(|h| !h.core.has_work())
    }
    /// shutdown：开始排空——指定 host 或全部 host 不再接纳新工作，完成在途。
    pub fn shutdown(&mut self, actor: Option<&str>) {
        // closed/closing 时直接早退，不能把 Closed 复活为 Draining。
        if matches!(self.state, HostState::Closing | HostState::Closed) {
            return;
        }
        match actor {
            Some(a) => {
                // 未知 host 必须报错，不能静默跳过。
                let host = self
                    .hosts
                    .get_mut(a)
                    .unwrap_or_else(|| panic!("unknown host: {}", a));
                host.core.begin_drain();
            }
            None => {
                // 54：全量 shutdown 把运行时标记为 draining。
                self.state = HostState::Draining;
                for host in self.hosts.values_mut() {
                    host.core.begin_drain();
                }
                self.front.begin_drain();
            }
        }
    }
    /// close：退役 host（指定或全部），返回未能结算的 abandoned 信封。
    /// infallible 包装；需处理落盘错误请用 `close_checked`。
    pub fn close(&mut self, actor: Option<&str>) -> Vec<crate::envelope::Envelope> {
        self.close_checked(actor).unwrap_or_default()
    }
    /// 可捕获错误的 close：137-172——落盘失败时保持
    /// Closing、保留失败 host、stash 已收集 roots 供下次 close 重试，不 panic。
    pub fn close_checked(&mut self, actor: Option<&str>) -> Result<Vec<crate::envelope::Envelope>> {
        if self.state == HostState::Closed {
            let mut out = core::mem::take(&mut self.abandoned);
            out.append(&mut self.stash);
            return Ok(out);
        }
        let targets: Vec<String> = match actor {
            Some(a) => {
                if !self.hosts.contains_key(a) {
                    return Err(crate::errors::EbaError::Other(format!(
                        "unknown host: {}",
                        a
                    )));
                }
                vec![a.to_string()]
            }
            None => self.hosts.keys().cloned().collect(),
        };
        let mut abandoned = core::mem::take(&mut self.abandoned);
        abandoned.append(&mut self.stash);
        // 全量 close 失败也释放 bus claim（保持 Closing
        // 供重试），避免已失败的 runtime 永久占用 bus owner。单 host close 不动 claim。
        let claim = if actor.is_none() {
            self.state = HostState::Closing;
            self.claim.take()
        } else {
            None
        };
        if actor.is_none() {
            match self
                .front
                .close_and_collect(&self.bus, saturating_now(self.clock.as_ref()))
            {
                Ok(roots) => abandoned.extend(roots),
                Err(error) => {
                    if let Some(token) = claim {
                        self.bus.release(token);
                    }
                    self.stash = abandoned;
                    return Err(error);
                }
            }
            abandoned.extend(self.bus.abandon_deliveries(None));
        }
        for a in targets {
            let pinned = saturating_now(self.clock.as_ref());
            let settled = {
                let bus = &self.bus;
                match self
                    .hosts
                    .get_mut(&a)
                    .expect("target present")
                    .core
                    .close_and_collect(bus, pinned)
                {
                    Ok(s) => s,
                    Err(error) => {
                        // 失败 host 保留在 hosts 中，roots stash 供下次重试。
                        if let Some(token) = claim {
                            self.bus.release(token);
                        }
                        self.stash = abandoned;
                        return Err(error);
                    }
                }
            };
            self.hosts.remove(&a);
            abandoned.extend(settled);
            self.bus.unsubscribe(&a);
            self.agenda.remove(&a);
        }
        if actor.is_none() {
            self.state = HostState::Closed;
            if let Some(token) = claim {
                self.bus.release(token);
            }
            // #front() 是 hosts Map 中的真实 host，全量 close 时一并退役：
            // stats().hosts 归零、hostsDetail 不含 sys.ask。
            self.front_activated = false;
            self.bus.unsubscribe("sys.ask");
            self.agenda.remove("sys.ask");
            self.front.sub.close();
        }
        Ok(abandoned)
    }
    /// 可捕获错误的 closeAsync：错误与 roots 都可见；失败时 roots 进入 stash，
    /// 下次 close 仍可返回。
    pub fn close_async_checked(
        &mut self,
        actor: Option<&str>,
        timeout_ms: usize,
    ) -> Result<Vec<crate::envelope::Envelope>> {
        self.shutdown(actor);
        let deadline = saturating_now(self.clock.as_ref()).saturating_add(timeout_ms as u64);
        let mut steps = 0;
        let budget = timeout_ms.max(1);
        while !self.is_quiescent()
            && steps < budget
            && saturating_now(self.clock.as_ref()) < deadline
        {
            if let Err(error) = self.advance() {
                let roots = self.close(actor);
                self.stash.extend(roots);
                return Err(error);
            }
            steps += 1;
        }
        let result = self.close_checked(actor)?;
        // 105-107：超时未静止时抛 "closeAsync timed out"，
        // 但 close 已完成（不回滚），roots 照常返回/stash。
        if !self.is_quiescent() && saturating_now(self.clock.as_ref()) >= deadline {
            return Err(crate::errors::EbaError::Clock(
                "closeAsync timed out".into(),
            ));
        }
        Ok(result)
    }
    /// closeAsync：无 async/executor，改为同步推进到静止。
    /// timeout_ms 由宿主 Clock 解释为墙钟预算；步数上限仅作
    /// 时钟不前进时的安全兜底。
    /// 返回 abandoned 信封与是否收敛；需处理错误请用 `close_async_checked`。
    pub fn close_async(
        &mut self,
        actor: Option<&str>,
        timeout_ms: usize,
    ) -> (Vec<crate::envelope::Envelope>, bool) {
        let converged_before = self.is_quiescent();
        let abandoned = self
            .close_async_checked(actor, timeout_ms)
            .unwrap_or_default();
        (abandoned, converged_before || self.is_quiescent())
    }
}
/// Drive：被宿主 tick 驱动的循环（无自带定时器、无 executor）。
pub struct Drive {
    runtime: Runtime,
    wake: Rc<crate::time::Wake>,
    closed: bool,
}
impl Drive {
    pub fn new(runtime: Runtime) -> Self {
        Self {
            runtime,
            wake: Rc::new(crate::time::Wake::new()),
            closed: false,
        }
    }
    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }
    pub fn kick(&self) {
        self.wake.fire();
    }
    pub fn wake(&self) -> &Rc<crate::time::Wake> {
        &self.wake
    }
    /// Drive 是否仍在运行。
    pub fn running(&self) -> bool {
        !self.closed
    }
    pub fn is_closed(&self) -> bool {
        self.closed
    }
    /// 关闭 Drive；Runtime 的关闭另行管理。
    pub fn close(&mut self) {
        self.closed = true;
    }
    /// 推进一步；返回的 Schedule 告诉宿主下次 tick 的时机。
    pub fn tick(&mut self) -> Result<Schedule> {
        if self.closed {
            return Ok(self.runtime.schedule());
        }
        self.runtime.advance()?;
        Ok(self.runtime.schedule())
    }
}
/// 把 front core 的调度面同步进 agenda（无需 `&mut Runtime`，便于与 `front` 分离借用）。
fn sync_front_parts(agenda: &mut Agenda, front: &Core) {
    let deadline = front.next_deadline_ms();
    let work = front.has_work();
    let active = work || front.state == HostState::Closing;
    agenda.update("sys.ask", deadline, active, work);
}
/// 把一个宿主 core 的调度面同步进 agenda。
fn sync_host_parts(agenda: &mut Agenda, actor: &str, core: &Core) {
    let deadline = core.next_deadline_ms();
    let work = core.has_work();
    let active = work || matches!(core.state, HostState::Closing | HostState::Draining);
    agenda.update(actor, deadline, active, work);
}
