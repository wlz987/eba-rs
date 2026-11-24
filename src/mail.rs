//! 邮件层：Bus / Inbox / Receiver / Routes / Queue。
//! ingress 顺序、request 单主、dedup、背压。
use crate::envelope::{Address, Envelope};
use crate::errors::{EbaError, Result};
use crate::pattern::Pattern;
use crate::prelude::*;
use crate::record::dedup_key;
use crate::schema::SchemaRegistry;
/// 有界 FIFO 队列。
struct Fifo<T> {
    items: VecDeque<T>,
    capacity: usize,
}
impl<T> Fifo<T> {
    fn new(capacity: usize) -> Self {
        Self {
            items: VecDeque::new(),
            capacity,
        }
    }
    fn can_enqueue(&self) -> bool {
        self.items.len() < self.capacity
    }
    fn push(&mut self, item: T) -> bool {
        if !self.can_enqueue() {
            return false;
        }
        self.items.push_back(item);
        true
    }
    /// latest 语义：先移除同键的旧项，再入队，保证队列中同键仅一条（V6）。
    fn push_latest(&mut self, item: T, same_key: impl Fn(&T) -> bool) -> bool {
        self.items.retain(|x| !same_key(x));
        self.push(item)
    }
    fn pop(&mut self) -> Option<T> {
        self.items.pop_front()
    }
    fn len(&self) -> usize {
        self.items.len()
    }
}
struct InboxState {
    fifo: Fifo<Envelope>,
    closed: bool,
    /// inbox.workPaused：暂停后拒绝 event/request，但 result 仍可落地。
    work_paused: bool,
    /// inbox.seized：Receiver.close 时未消费的僵信归入此处，供租约回收。
    seized: Vec<Envelope>,
    /// 反查该 Inbox 被多少路由订阅持有（routes refcount）。
    route_refs: usize,
    /// 入队即唤醒的信号。调度器据此只推进被唤醒的宿主，而不是每轮全扫。
    signal: Option<Rc<crate::notify::Signal>>,
}
/// 收件箱（有界 FIFO）。
#[derive(Clone)]
pub struct Inbox {
    state: Rc<RefCell<InboxState>>,
}
impl Inbox {
    pub fn new(capacity: usize) -> Self {
        Self {
            state: Rc::new(RefCell::new(InboxState {
                fifo: Fifo::new(capacity),
                closed: false,
                work_paused: false,
                seized: Vec::new(),
                route_refs: 0,
                signal: None,
            })),
        }
    }
    pub fn capacity(&self) -> usize {
        self.state.borrow().fifo.capacity
    }
    pub fn remaining(&self) -> usize {
        let s = self.state.borrow();
        // closed inbox 剩余容量恒为 0。
        if s.closed {
            return 0;
        }
        s.fifo.capacity - s.fifo.len()
    }
    pub fn is_closed(&self) -> bool {
        self.state.borrow().closed
    }
    pub fn close(&mut self) {
        self.state.borrow_mut().closed = true;
    }
    pub fn receiver(&self) -> Receiver {
        Receiver {
            state: self.state.clone(),
            active: Cell::new(true),
            borrows: Cell::new(0),
        }
    }
    /// Caller.admit 的 inbox 容量门控：runtime 在 admit 前查询 request 是否有位。
    pub fn can_enqueue(&self, kind: &str) -> bool {
        let s = self.state.borrow();
        if s.closed || !s.fifo.can_enqueue() {
            return false;
        }
        // 暂停只挡 event/request，diagnostic 仍可入队。
        !(s.work_paused && matches!(kind, "event" | "request"))
    }
    /// 暂停期间拒收新工作，回信仍可落地。
    pub fn pause_work(&self) {
        self.state.borrow_mut().work_paused = true;
    }
    pub fn resume_work(&self) {
        let (was_paused, signal) = {
            let mut s = self.state.borrow_mut();
            let was = s.work_paused;
            s.work_paused = false;
            (was, s.signal.clone())
        };
        // 仅从 paused 恢复时才通知，幂等调用不产生多余唤醒。
        if was_paused {
            if let Some(sig) = signal {
                sig.notify(crate::notify::READ | crate::notify::WORK);
            }
        }
    }
    pub fn route_refs(&self) -> usize {
        self.state.borrow().route_refs
    }
    /// 绑定唤醒信号：此后每次入队都通知调度器（READ|WORK）。
    pub fn bind_signal(&self, signal: Rc<crate::notify::Signal>) {
        self.state.borrow_mut().signal = Some(signal);
    }
    /// deliver 跳过绑定给调用方自己的 inbox。
    pub fn bound_to(&self, home: u64) -> bool {
        self.state
            .borrow()
            .signal
            .as_ref()
            .is_some_and(|s| s.bound_to(home))
    }
    fn enqueue(&self, envelope: Envelope) -> bool {
        let mut s = self.state.borrow_mut();
        if !s.fifo.push(envelope) {
            return false;
        }
        // 入队即通知：调度器无需逐轮询问"有没有新信"。
        if let Some(sig) = &s.signal {
            sig.notify(crate::notify::READ | crate::notify::WORK);
        }
        true
    }
    /// 撤销时从收件箱摘除僵信，
    /// 防止 phase_read 重新 adopt 已撤销的 request。
    pub fn remove_by_id(&self, id: &crate::envelope::EnvelopeId) -> bool {
        let mut s = self.state.borrow_mut();
        let before = s.fifo.len();
        s.fifo.items.retain(|e| &e.header.id != id);
        before != s.fifo.len()
    }
    /// 同 topic 的事件合并：旧事件被最新事件替换（V6 publish_latest）。
    fn enqueue_latest(&self, envelope: Envelope) -> bool {
        let topic = envelope.header.topic.clone();
        let mut s = self.state.borrow_mut();
        let ok = s.fifo.push_latest(envelope, |e| {
            matches!(e.header.address, Address::Event) && e.header.topic == topic
        });
        if ok {
            if let Some(sig) = &s.signal {
                sig.notify(crate::notify::READ | crate::notify::WORK);
            }
        }
        ok
    }
}
/// 收信器：可重入 borrow 计数；take 转移所有权而不关闭 inbox。
pub struct Receiver {
    state: Rc<RefCell<InboxState>>,
    active: Cell<bool>,
    borrows: Cell<usize>,
}
impl Receiver {
    pub fn is_active(&self) -> bool {
        self.active.get() && !self.state.borrow().closed
    }
    /// 可重入借用：返回是否成功 的计数器语义）。
    pub fn borrow(&self) -> bool {
        if !self.is_active() {
            return false;
        }
        self.borrows.set(self.borrows.get() + 1);
        true
    }
    pub fn unborrow(&self) {
        self.borrows.set(self.borrows.get().saturating_sub(1));
    }
    pub fn borrowed(&self) -> usize {
        self.borrows.get()
    }
    pub fn try_recv(&self) -> Option<Envelope> {
        // 只 requireActive，不要求 borrows>0；
        // Subscriber.start 借后立即释放，pump 仍可正常收信。
        if !self.is_active() {
            return None;
        }
        let (env, signal, emptied) = {
            let mut s = self.state.borrow_mut();
            let env = s.fifo.pop();
            (env, s.signal.clone(), s.fifo.items.is_empty())
        };
        // 队列清空时清 READ，撤销陈旧唤醒。
        if emptied {
            if let Some(sig) = signal {
                sig.clear(crate::notify::READ);
            }
        }
        env
    }
    /// Receiver.close：未消费信件移入 seized，而不是直接丢弃。
    pub fn close(&self) -> bool {
        let started = self.active.get();
        let mut s = self.state.borrow_mut();
        if started {
            self.active.set(false);
        } else if !s.closed {
            // take 后的旧 receiver 已失活，关闭必须早退，
            // 否则会误关 inbox 并把新 receiver 的队列抽空。
            return false;
        }
        if !s.closed {
            s.closed = true;
            let drained: Vec<Envelope> = s.fifo.items.drain(..).collect();
            s.seized.extend(drained);
        }
        let signal = s.signal.clone();
        drop(s);
        // close 时清 READ，撤销该 inbox 的陈旧唤醒。
        if let Some(sig) = signal {
            sig.clear(crate::notify::READ);
        }
        started
    }
    /// Receiver.take：转移所有权到新 Receiver，旧 receiver 失效但 inbox 仍开、
    /// 队列原地保留（不再被关闭/抽空）。
    pub fn take(&self) -> Receiver {
        assert!(self.is_active(), "receiver transferred or closed");
        assert_eq!(self.borrows.get(), 0, "receiver is borrowed");
        self.active.set(false);
        Receiver {
            state: self.state.clone(),
            active: Cell::new(true),
            borrows: Cell::new(0),
        }
    }
}
#[derive(Clone)]
struct Subscription {
    actor: String,
    pattern: Pattern,
    inbox: Inbox,
    /// route entry.count：同一 (inbox, pattern) 重复订阅的份数。
    count: usize,
}
/// 路由表：按 topic 精确/通配匹配订阅者。
pub struct Routes {
    subs: Vec<Subscription>,
}
impl Routes {
    fn new() -> Self {
        Self { subs: Vec::new() }
    }
    fn subscribe(&mut self, actor: &str, pattern: Pattern, inbox: Inbox) {
        // 同一 (inbox, pattern) 重复订阅时
        // entry.count += 1，路由表仍只有一个 entry（只投递一份），只有首次
        // 才通知 changes。RS 用 route_refs 记这个 count。
        let duplicate = self.subs.iter_mut().find(|s| {
            s.actor == actor
                && s.pattern.literal == pattern.literal
                && s.pattern.wildcard == pattern.wildcard
                && Rc::ptr_eq(&s.inbox.state, &inbox.state)
        });
        if let Some(existing) = duplicate {
            existing.inbox.state.borrow_mut().route_refs += 1;
            existing.count += 1;
            return;
        }
        inbox.state.borrow_mut().route_refs += 1;
        self.subs.push(Subscription {
            actor: actor.to_string(),
            pattern,
            inbox,
            count: 1,
        });
    }
    fn unsubscribe(&mut self, actor: &str) {
        self.subs.retain(|s| {
            if s.actor == actor {
                // detach 每份 count 归还一次。
                let refs = s.inbox.state.borrow().route_refs.saturating_sub(s.count);
                s.inbox.state.borrow_mut().route_refs = refs;
                false
            } else {
                true
            }
        });
    }
    /// 命中的订阅者（借用视图，不克隆 Inbox）。
    /// Receiver.close 会 unsubscribeAll 移除路由；RS 没有 back-reference，
    /// 用「跳过已关闭 inbox」达到同样的可观察行为（不向僵尸路由投递）。
    fn matches<'a>(&'a self, topic: &'a str) -> impl Iterator<Item = &'a Subscription> + 'a {
        self.subs
            .iter()
            .filter(move |s| s.pattern.matches(topic) && !s.inbox.is_closed())
    }
    /// snapshotMatch：按 inbox 身份去重。同一 inbox 以多个
    /// 重叠 pattern 订阅时只算一个目标，否则 request 会误报 AmbiguousRoute、
    /// event 会重复入队。
    fn distinct_matches(&self, topic: &str) -> Vec<&Subscription> {
        let mut out: Vec<&Subscription> = Vec::new();
        for sub in &self.subs {
            if !sub.pattern.matches(topic) || sub.inbox.is_closed() {
                continue;
            }
            if !out
                .iter()
                .any(|seen| Rc::ptr_eq(&seen.inbox.state, &sub.inbox.state))
            {
                out.push(sub);
            }
        }
        out
    }
    fn match_count(&self, topic: &str) -> usize {
        self.distinct_matches(topic).len()
    }
    /// topic 在该 inbox 上的订阅引用计数。
    fn subscription_count(&self, topic: &str, inbox: &Inbox) -> usize {
        // bus.subscriptionCount 累加 entry.count（重复订阅返回 N）。
        self.matches(topic)
            .filter(|s| Rc::ptr_eq(&s.inbox.state, &inbox.state))
            .map(|s| s.count)
            .sum()
    }
}
pub struct TrustPolicy {
    allows: Box<dyn Fn(&str) -> bool>,
}
impl TrustPolicy {
    pub fn new(allows: Box<dyn Fn(&str) -> bool>) -> Self {
        Self { allows }
    }
    pub fn allows(&self, from: &str) -> bool {
        (self.allows)(from)
    }
}
/// 单写者 claim token。
pub struct ClaimToken(u64);
/// 总线：投递与路由的中心。
#[derive(Clone)]
pub struct Bus {
    routes: Rc<RefCell<Routes>>,
    dedup: Rc<RefCell<crate::record::DedupWindow>>,
    schemas: Option<Rc<SchemaRegistry>>,
    trust: Rc<RefCell<Option<TrustPolicy>>>,
    owner: RefCell<Option<u64>>,
    token_seq: Cell<u64>,
    issued_token: Cell<Option<u64>>,
    /// publish 失败时暂存信封，
    /// flush_pending 在 advance 中重试（满则保留，无路由则交还调用方）。
    pending_deliveries: RefCell<Vec<(u64, Envelope)>>,
    /// DeliveryWatch 无路由时归还原 owner 的 seized 信件。
    seized: RefCell<BTreeMap<u64, Vec<Envelope>>>,
    /// holdWait/releaseWait 维护的等待 requestId 集合。
    waits: RefCell<BTreeSet<String>>,
}
impl Bus {
    /// 只交还属于指定 home 的 pending/seized 信件。
    pub fn abandon_deliveries(&self, home: Option<u64>) -> Vec<Envelope> {
        let pending = core::mem::take(&mut *self.pending_deliveries.borrow_mut());
        let mut seized = self.seized.borrow_mut();
        // 先收集 pending 涉及的 home：None 分支必须包含它们，否则首轮尚未
        // 产生 seized 桶的 home 的 pending 信既没返回也留在表里（历史丢信 bug）。
        let mut homes: BTreeSet<u64> = seized.keys().copied().collect();
        for (owner_h, _) in &pending {
            homes.insert(*owner_h);
        }
        let keys: Vec<u64> = match home {
            Some(h) => vec![h],
            None => homes.into_iter().collect(),
        };
        let mut out = Vec::new();
        let mut kept_pending = Vec::new();
        for (owner_h, env) in pending {
            if home.is_none() || home == Some(owner_h) {
                out.push(env);
            } else {
                // abandonDeliveries 只处理指定 home 的 watch，其它 home 的
                // offer 保留在 $deliveries 继续重试——放回 pending 而不是 seized。
                kept_pending.push((owner_h, env));
            }
        }
        for h in keys {
            if let Some(letters) = seized.remove(&h) {
                out.extend(letters);
            }
        }
        drop(seized);
        self.pending_deliveries.borrow_mut().extend(kept_pending);
        out
    }
    pub fn new() -> Self {
        Self::with_schemas(None)
    }
    pub fn with_schemas(schemas: Option<Rc<SchemaRegistry>>) -> Self {
        Self {
            routes: Rc::new(RefCell::new(Routes::new())),
            dedup: Rc::new(RefCell::new(crate::record::DedupWindow::new(1024))),
            schemas,
            trust: Rc::new(RefCell::new(None)),
            owner: RefCell::new(None),
            token_seq: Cell::new(1),
            issued_token: Cell::new(None),
            pending_deliveries: RefCell::new(Vec::new()),
            seized: RefCell::new(BTreeMap::new()),
            waits: RefCell::new(BTreeSet::new()),
        }
    }
    pub fn set_trust_policy(&self, policy: Option<TrustPolicy>) {
        *self.trust.borrow_mut() = policy;
    }
    pub fn subscribe(&self, actor: &str, pattern: Pattern, inbox: Inbox) {
        self.routes.borrow_mut().subscribe(actor, pattern, inbox);
    }
    pub fn unsubscribe(&self, actor: &str) {
        self.routes.borrow_mut().unsubscribe(actor);
    }
    pub fn subscription_count(&self, topic: &str, inbox: &Inbox) -> usize {
        self.routes.borrow().subscription_count(topic, inbox)
    }
    /// 有 owner 时拒绝登记（纯读操作不能改状态）。
    pub fn hold_wait(&self, request_id: impl Into<String>) -> bool {
        if self.owner.borrow().is_some() {
            return false;
        }
        self.waits.borrow_mut().insert(request_id.into());
        true
    }
    pub fn release_wait(&self, request_id: &str) -> bool {
        if self.owner.borrow().is_some() {
            return false;
        }
        // Set.delete 幂等，恒返回 true。
        self.waits.borrow_mut().remove(request_id);
        true
    }
    pub fn waiting(&self, request_id: Option<&str>) -> bool {
        request_id.is_some_and(|id| self.waits.borrow().contains(id))
    }
    /// claim 把单写者所有权交给一个 owner（运行时 id），返回一次性 token。
    pub fn claim(&self, owner_id: u64) -> Result<ClaimToken> {
        if self.owner.borrow().is_some() {
            return Err(EbaError::Reentrant(
                "bus already belongs to a runtime".into(),
            ));
        }
        *self.owner.borrow_mut() = Some(owner_id);
        let t = self.token_seq.get();
        self.token_seq.set(t + 1);
        self.issued_token.set(Some(t));
        Ok(ClaimToken(t))
    }
    pub fn release(&self, token: ClaimToken) {
        // 只有当前 claim token 才能释放，过期/外部 token 无效。
        if self.issued_token.get() == Some(token.0) {
            *self.owner.borrow_mut() = None;
            self.issued_token.set(None);
        }
    }
    pub fn owner(&self) -> Option<u64> {
        *self.owner.borrow()
    }
    fn migrate<'a>(&'a self, envelope: &'a Envelope) -> Result<alloc::borrow::Cow<'a, Envelope>> {
        // 无 schema 迁移时直接借用原信封：投递本身不会改写信封，深拷贝纯属浪费。
        // 只有注册了 schema 且需要升版时才复制一份改写后的副本。
        match &self.schemas {
            Some(s) => Ok(alloc::borrow::Cow::Owned(s.migrate_envelope(envelope)?)),
            None => Ok(alloc::borrow::Cow::Borrowed(envelope)),
        }
    }
    fn check_trust(&self, envelope: &Envelope) -> Result<()> {
        // 拒绝必须是
        // EnvelopeBuildError，而非 InvalidTopic。
        if let Some(policy) = self.trust.borrow().as_ref() {
            envelope.check_trust(policy)?;
        }
        Ok(())
    }
    /// publish：request 必须恰好一个收件人；满则 MailboxFull；dedup 后落地。
    pub fn publish(&self, envelope: &Envelope) -> Result<bool> {
        let snap = self.migrate(envelope)?;
        self.check_trust(&snap)?;
        let count = self.routes.borrow().match_count(&snap.header.topic);
        if count == 0 {
            return Ok(false);
        }
        if matches!(snap.header.address, Address::Request { .. }) && count > 1 {
            return Err(EbaError::AmbiguousRoute(format!(
                "request topic {} matches {} subscribers",
                snap.header.topic, count
            )));
        }
        let key = dedup_key(&snap.header.from, &snap.header.id);
        if self.dedup.borrow().has(&key) {
            return Ok(false);
        }
        // 目标借用视图：先做容量检查，再逐封投递，全程不克隆 Inbox 列表。
        {
            let routes = self.routes.borrow();
            let kind = snap.header.address.kind();
            let targets = routes.distinct_matches(&snap.header.topic);
            for sub in &targets {
                if !sub.inbox.can_enqueue(kind) {
                    return Err(EbaError::MailboxFull("target inbox full".into()));
                }
            }
            for sub in &targets {
                sub.inbox.enqueue(snap.clone().into_owned());
            }
        }
        self.dedup.borrow_mut().accept(key);
        Ok(true)
    }
    /// tell：best-effort fanout，满则跳过并计入 dropped；
    /// migrate/trust 失败是 fail-closed 硬错误，必须抛给调用方。
    pub fn tell(&self, envelope: &Envelope) -> Result<usize> {
        let snap = self.migrate(envelope)?;
        self.check_trust(&snap)?;
        let routes = self.routes.borrow();
        let kind = snap.header.address.kind();
        let mut dropped = 0;
        for sub in routes.distinct_matches(&snap.header.topic) {
            if sub.inbox.can_enqueue(kind) {
                sub.inbox.enqueue(snap.clone().into_owned());
            } else {
                dropped += 1;
            }
        }
        Ok(dropped)
    }
    /// publish_latest：同 topic 事件合并——目标队列中只保留最新一条（V6 / E3）。
    /// 与 tell 不同：tell 每发必入队，publish_latest 会替换同 topic 的旧事件。
    pub fn publish_latest(&self, envelope: &Envelope) -> Result<bool> {
        let snap = self.migrate(envelope)?;
        self.check_trust(&snap)?;
        let count = self.routes.borrow().match_count(&snap.header.topic);
        if count == 0 {
            return Ok(false);
        }
        let routes = self.routes.borrow();
        let kind = snap.header.address.kind();
        let targets = routes.distinct_matches(&snap.header.topic);
        for sub in &targets {
            if !sub.inbox.can_enqueue(kind) {
                return Err(EbaError::MailboxFull("target inbox full".into()));
            }
        }
        for sub in &targets {
            sub.inbox.enqueue_latest(snap.clone().into_owned());
        }
        Ok(true)
    }
    /// bus.place：deliver 专用投递——跳过 bound-to-home 与 closed 的 inbox，
    /// 不做 dedup、不查 trust；满则 MailboxFull，零目标返回 false。
    /// `Ok(None)` 表示 migrate 失败——bus.deliver 静默丢弃，绝不能归入 seized。
    fn place_deliver(&self, envelope: &Envelope, home: u64) -> Result<Option<bool>> {
        // deliver 只做 migrate，且失败静默丢弃（不查 trust、不 dedup）。
        let snap = match self.migrate(envelope) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        let kind = snap.header.address.kind();
        let routes = self.routes.borrow();
        let targets: Vec<&Subscription> = routes
            .distinct_matches(&snap.header.topic)
            .into_iter()
            .filter(|sub| !sub.inbox.bound_to(home))
            .collect();
        if targets.is_empty() {
            return Ok(Some(false));
        }
        for sub in &targets {
            if !sub.inbox.can_enqueue(kind) {
                return Err(EbaError::MailboxFull("target inbox full".into()));
            }
        }
        for sub in &targets {
            sub.inbox.enqueue(snap.clone().into_owned());
        }
        Ok(Some(true))
    }
    /// 满则按 home 暂存等待重试；
    /// 无路由或 ingress 失败则归入该 home 的 seized，供 abandonDeliveries 归还。
    pub fn deliver(&self, envelope: &Envelope, home: u64) -> Result<bool> {
        match self.place_deliver(envelope, home) {
            Ok(Some(true)) => Ok(true),
            Ok(None) => {
                // bus.deliver:221-227：migrate 失败直接 return，不 arm offer。
                Ok(false)
            }
            Ok(Some(false)) => {
                self.seized
                    .borrow_mut()
                    .entry(home)
                    .or_default()
                    .push(envelope.clone());
                Ok(false)
            }
            Err(EbaError::MailboxFull(_)) => {
                self.pending_deliveries
                    .borrow_mut()
                    .push((home, envelope.clone()));
                Ok(false)
            }
            Err(_) => {
                // deliver 吞掉 ingress 错误；信件交还 owner 而不是静默丢失。
                self.seized
                    .borrow_mut()
                    .entry(home)
                    .or_default()
                    .push(envelope.clone());
                Ok(false)
            }
        }
    }
    /// 重试待投递信封；返回 (成功数, 仍积压数)。advance 开头调用。
    pub fn flush_pending_deliveries(&self) -> (usize, usize) {
        let pending = core::mem::take(&mut *self.pending_deliveries.borrow_mut());
        let mut ok = 0;
        for (home, env) in pending {
            match self.place_deliver(&env, home) {
                Ok(Some(true)) => ok += 1,
                Ok(None) => {} // migrate 失败：静默丢弃
                Err(EbaError::MailboxFull(_)) => {
                    self.pending_deliveries.borrow_mut().push((home, env))
                }
                // 无路由/硬错误：交还 home 的 seized，而不是永久滞留。
                _ => self.seized.borrow_mut().entry(home).or_default().push(env),
            }
        }
        (ok, self.pending_deliveries.borrow().len())
    }
}
impl Default for Bus {
    fn default() -> Self {
        Self::new()
    }
}
