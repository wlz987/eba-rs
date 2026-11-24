//! 通知与调度：Signal / Interest / Watchers / Agenda。
use crate::prelude::*;
use crate::time::{Deadlines, Schedule};
use alloc::rc::Weak;
pub const READ: u32 = 1;
pub const TIMER: u32 = 2;
pub const WORK: u32 = 4;
pub const DELIVERY: u32 = 8;
/// 四相信号旗标。宿主绑定用 actor id 比较（代替 WeakRef）。
#[derive(Clone)]
pub struct Signal {
    flags: Cell<u32>,
    owner: RefCell<Option<(u64, String)>>,
    /// 绑定时注册的同步唤醒回调。
    wake: RefCell<Option<Rc<dyn Fn()>>>,
    /// 与 wake 方向相反。
    unwake: RefCell<Option<Rc<dyn Fn()>>>,
    /// bind 重绑时触发旧 owner 的 unbindSignal 回调。
    unbind: RefCell<Option<Rc<dyn Fn()>>>,
}
impl Signal {
    pub fn new() -> Self {
        Self {
            flags: Cell::new(0),
            owner: RefCell::new(None),
            wake: RefCell::new(None),
            unwake: RefCell::new(None),
            unbind: RefCell::new(None),
        }
    }
    pub fn flags(&self) -> u32 {
        self.flags.get()
    }
    pub fn has(&self, flag: u32) -> bool {
        self.flags.get() & flag != 0
    }
    pub fn notify(&self, flag: u32) {
        self.flags.set(self.flags.get() | flag);
        // 未绑定时只记旗标，不唤醒任何 owner。
        if self.is_bound() {
            if let Some(wake) = self.wake.borrow().as_ref() {
                wake();
            }
        }
    }
    pub fn clear(&self, flag: u32) {
        let next = self.flags.get() & !flag;
        self.flags.set(next);
        // flags 归零时 unqueue（方向必须与 notify 相反）。
        if next == 0 && self.is_bound() {
            if let Some(unwake) = self.unwake.borrow().as_ref() {
                unwake();
            }
        }
    }
    /// 注册同步唤醒回调（Runtime 绑定 signal 到 Agenda 时设置）。
    pub fn set_wake(&self, wake: Rc<dyn Fn()>) {
        *self.wake.borrow_mut() = Some(wake);
    }
    pub fn set_unwake(&self, unwake: Rc<dyn Fn()>) {
        *self.unwake.borrow_mut() = Some(unwake);
    }
    pub fn set_unbind(&self, unbind: Rc<dyn Fn()>) {
        *self.unbind.borrow_mut() = Some(unbind);
    }
    pub fn set(&self, flag: u32, on: bool) {
        if on {
            self.notify(flag);
        } else {
            self.clear(flag);
        }
    }
    pub fn bind(&self, owner_id: u64, actor: impl Into<String>) {
        let prior = self.owner.borrow().as_ref().map(|(id, _)| *id);
        if prior.is_some_and(|id| id != owner_id) {
            if let Some(unbind) = self.unbind.borrow().as_ref() {
                unbind();
            }
        }
        *self.owner.borrow_mut() = Some((owner_id, actor.into()));
        // 重绑时把当前旗标重放到新 owner。
        if self.flags.get() != 0 {
            if let Some(wake) = self.wake.borrow().as_ref() {
                wake();
            }
        }
    }
    pub fn is_bound(&self) -> bool {
        self.owner.borrow().is_some()
    }
    pub fn bound_to(&self, owner_id: u64) -> bool {
        self.owner
            .borrow()
            .as_ref()
            .map(|(id, _)| *id == owner_id)
            .unwrap_or(false)
    }
    pub fn actor(&self) -> Option<String> {
        self.owner.borrow().as_ref().map(|(_, a)| a.clone())
    }
    pub fn detach(&self) {
        if self.is_bound() {
            if let Some(unbind) = self.unbind.borrow().as_ref() {
                unbind();
            }
        }
        *self.owner.borrow_mut() = None;
    }
}
impl Default for Signal {
    fn default() -> Self {
        Self::new()
    }
}
/// 兴趣：等待某个 topic 的投递/状态变化。
pub struct Interest {
    key: u64,
    topic: String,
    signal: Rc<Signal>,
    owners: RefCell<BTreeSet<u64>>,
    /// 通知时登记 topic 供背压重投使用。
    candidates: RefCell<BTreeSet<String>>,
}
impl Interest {
    pub fn new(key: u64, topic: impl Into<String>, signal: Rc<Signal>) -> Self {
        Self {
            key,
            topic: topic.into(),
            signal,
            owners: RefCell::new(BTreeSet::new()),
            candidates: RefCell::new(BTreeSet::new()),
        }
    }
    pub fn key(&self) -> u64 {
        self.key
    }
    pub fn topic(&self) -> &str {
        &self.topic
    }
    pub fn signal(&self) -> &Rc<Signal> {
        &self.signal
    }
    pub fn notify(&self) {
        self.candidates.borrow_mut().insert(self.topic.clone());
        self.signal.notify(DELIVERY);
    }
    pub fn owners_len(&self) -> usize {
        self.owners.borrow().len()
    }
    fn add_owner(&self, id: u64) {
        self.owners.borrow_mut().insert(id);
    }
    fn remove_owner(&self, id: u64) {
        self.owners.borrow_mut().remove(&id);
    }
}
/// 观察者集合：多个 Interest 订阅同一变更源。
pub struct Watchers {
    id: u64,
    /// 弱引用避免 Interest 生命周期被 watcher 拉长。
    waiting: RefCell<BTreeMap<u64, Weak<Interest>>>,
    on_empty: Option<Box<dyn Fn()>>,
}
impl Watchers {
    pub fn new(id: u64, on_empty: Option<Box<dyn Fn()>>) -> Self {
        Self {
            id,
            waiting: RefCell::new(BTreeMap::new()),
            on_empty,
        }
    }
    pub fn watch(&self, interest: &Rc<Interest>) {
        self.waiting
            .borrow_mut()
            .insert(interest.key(), Rc::downgrade(interest));
        interest.add_owner(self.id);
    }
    pub fn watched(&self) -> usize {
        self.waiting.borrow().len()
    }
    pub fn live(&self) -> Vec<Rc<Interest>> {
        // 顺带清理已失效条目。
        let mut dead = Vec::new();
        let mut alive = Vec::new();
        for (k, w) in self.waiting.borrow().iter() {
            match w.upgrade() {
                Some(i) => alive.push(i),
                None => dead.push(*k),
            }
        }
        if !dead.is_empty() {
            let mut map = self.waiting.borrow_mut();
            for k in dead {
                map.remove(&k);
            }
        }
        alive
    }
    pub fn remove(&self, interest: &Interest) -> bool {
        let removed = self.waiting.borrow_mut().remove(&interest.key()).is_some();
        if removed {
            interest.remove_owner(self.id);
            if self.waiting.borrow().is_empty() {
                if let Some(hook) = &self.on_empty {
                    hook();
                }
            }
        }
        removed
    }
    pub fn notify(&self) {
        // waiting 存 Weak：升级失败的条目已失效，直接跳过。
        let snapshot: Vec<Rc<Interest>> = self
            .waiting
            .borrow()
            .values()
            .filter_map(|w| w.upgrade())
            .collect();
        if snapshot.is_empty() {
            return;
        }
        self.waiting.borrow_mut().clear();
        for i in &snapshot {
            i.remove_owner(self.id);
        }
        if let Some(hook) = &self.on_empty {
            hook();
        }
        for i in snapshot {
            i.notify();
        }
    }
}
/// 议程：哪些 actor 有 ready work、活动集合、deadline 堆。
/// 过滤失效条目并顺带清理。
/// 内部以**稳定 u64 id** 为键：actor 名只在首次登记时分配一次，此后每轮
/// `update` 只做一次查表，不再逐轮 `to_string()`。这消除了一次推进里
/// 每个宿主一次的小字符串分配——那是空转推进最大的固定开销来源。
pub struct Agenda {
    ids: BTreeMap<String, u64>,
    names: Vec<Option<String>>,
    ready: BTreeSet<u64>,
    /// ready 集合只判重，FIFO 队列决定 pop 顺序。
    ready_order: VecDeque<u64>,
    active: BTreeSet<u64>,
    deadlines: Deadlines<u64>,
    signals: BTreeMap<u64, Rc<Signal>>,
    next_id: u64,
    /// Agenda.bindWake 捕获 wake 回调错误，由 advance 包装成 WakeError。
    fault: RefCell<Option<String>>,
}
impl Agenda {
    /// 取出 wake 回调记录的 fault。
    pub fn take_fault(&self) -> Option<String> {
        self.fault.borrow_mut().take()
    }
    /// wake 回调内可主动记录 fault，advance 会把它包装成 WakeError 抛出。
    pub fn set_fault(&self, message: impl Into<String>) {
        *self.fault.borrow_mut() = Some(message.into());
    }
    pub fn new() -> Self {
        Self {
            ids: BTreeMap::new(),
            names: Vec::new(),
            ready: BTreeSet::new(),
            ready_order: VecDeque::new(),
            active: BTreeSet::new(),
            deadlines: Deadlines::new(),
            signals: BTreeMap::new(),
            next_id: 0,
            fault: RefCell::new(None),
        }
    }
    /// 登记 actor 名到稳定 id；已登记则直接复用，不产生分配。
    fn intern(&mut self, actor: &str) -> u64 {
        if let Some(id) = self.ids.get(actor) {
            return *id;
        }
        let id = self.next_id;
        self.next_id += 1;
        self.ids.insert(actor.to_string(), id);
        self.names.push(Some(actor.to_string()));
        id
    }
    fn lookup(&self, actor: &str) -> Option<u64> {
        self.ids.get(actor).copied()
    }
    /// 已登记 actor 的稳定 id（供唤醒回调直接使用，避免克隆名字）。
    pub fn id_of(&self, actor: &str) -> Option<u64> {
        self.lookup(actor)
    }
    pub fn bind_signal(&mut self, actor: &str, signal: Rc<Signal>) {
        let id = self.intern(actor);
        self.signals.insert(id, signal);
    }
    pub fn unbind_signal(&mut self, actor: &str) {
        if let Some(id) = self.lookup(actor) {
            self.signals.remove(&id);
            self.unqueue_ready(id);
            self.active.remove(&id);
        }
    }
    /// 入队：集合判重 + FIFO 顺序。
    fn enqueue_ready(&mut self, id: u64) {
        if self.ready.insert(id) {
            self.ready_order.push_back(id);
        }
    }
    fn unqueue_ready(&mut self, id: u64) {
        self.ready.remove(&id);
    }
    pub fn enqueue(&mut self, actor: &str) {
        let id = self.intern(actor);
        self.active.insert(id);
        self.enqueue_ready(id);
    }
    /// 已持有稳定 id 时直接入队，不查名字表。
    pub fn enqueue_id(&mut self, id: u64) {
        self.active.insert(id);
        self.enqueue_ready(id);
    }
    pub fn unqueue(&mut self, actor: &str) {
        if let Some(id) = self.lookup(actor) {
            self.ready.remove(&id);
        }
    }
    pub fn unqueue_id(&mut self, id: u64) {
        self.ready.remove(&id);
    }
    pub fn ready_size(&self) -> usize {
        self.ready.len()
    }
    pub fn pop(&mut self) -> Option<String> {
        // 跳过已被 unqueue 的陈旧 id。
        let id = loop {
            let id = self.ready_order.pop_front()?;
            if self.ready.remove(&id) {
                break id;
            }
        };
        self.names.get(id as usize).and_then(|n| n.clone())
    }
    pub fn next_deadline_ms(&self) -> Option<u64> {
        self.deadlines.next_ms()
    }
    /// 到点：移除 deadline 并给对应 signal 打 TIMER 旗标。
    pub fn due(&mut self, now_ms: u64) {
        while let Some(id) = self.deadlines.peek_due(now_ms) {
            self.deadlines.remove(&id);
            if let Some(sig) = self.signals.get(&id) {
                sig.notify(TIMER);
            }
        }
    }
    pub fn update(&mut self, actor: &str, deadline: Option<u64>, active: bool, ready: bool) {
        let id = self.intern(actor);
        // 330-334：仅当 deadline 变化时才 remove+add，
        // 相同 deadline 保留原插入序号（避免因 seq 自增被移到同组队尾）。
        if let Some(d) = deadline {
            let d = crate::time::safe_deadline(d).expect("agenda deadline must be safe");
            if self.deadlines.at(&id) != Some(d) {
                self.deadlines.remove(&id);
                let _ = self.deadlines.add(id, d);
            }
        } else {
            self.deadlines.remove(&id);
        }
        if active {
            self.active.insert(id);
        } else {
            self.active.remove(&id);
        }
        if ready {
            self.enqueue_ready(id);
        } else {
            self.unqueue_ready(id);
        }
    }
    pub fn remove(&mut self, actor: &str) {
        if let Some(id) = self.lookup(actor) {
            if let Some(sig) = self.signals.get(&id) {
                sig.detach();
            }
            self.signals.remove(&id);
            self.unqueue_ready(id);
            self.active.remove(&id);
            self.deadlines.remove(&id);
            self.ids.remove(actor);
            if let Some(slot) = self.names.get_mut(id as usize) {
                *slot = None;
            }
        }
    }
    pub fn schedule(&self, now_ms: u64) -> Schedule {
        let deadline = self.deadlines.next_ms();
        Schedule::new(
            !self.ready.is_empty() || deadline.map(|d| d <= now_ms).unwrap_or(false),
            deadline,
            self.active.is_empty(),
            Some(now_ms),
        )
    }
}
impl Default for Agenda {
    fn default() -> Self {
        Self::new()
    }
}
