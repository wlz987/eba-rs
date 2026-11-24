//! 时间与调度。
//!
//! 关键：续跑读 live clock 而非 hook 创建时的 pinned clock——否则 t=500 恢复时
//! 会用 t=0 算 deadline，立即过期。
use crate::errors::{EbaError, Result};
use crate::prelude::*;
use core::cmp::Ordering;
pub const MAX_TIMER_DELAY_MS: u64 = 2_147_483_647;
/// 一次性唤醒闩锁 + 单 waiter。
/// `disarm`/`expire` 按 Rc 身份比对。
pub struct WakeTicket {
    fired: Cell<bool>,
    waiter: RefCell<Option<Rc<dyn Fn()>>>,
}
impl WakeTicket {
    pub fn new() -> Self {
        Self {
            fired: Cell::new(false),
            waiter: RefCell::new(None),
        }
    }
    pub fn fire(&self) {
        self.fired.set(true);
        let waiter = self.waiter.borrow().clone();
        if let Some(w) = waiter {
            w();
        }
    }
    pub fn consume(&self) -> bool {
        let fired = self.fired.get();
        self.fired.set(false);
        fired
    }
    pub fn arm(&self, waiter: Rc<dyn Fn()>) {
        *self.waiter.borrow_mut() = Some(waiter.clone());
        if self.fired.get() {
            waiter();
        }
    }
    pub fn disarm(&self, waiter: &Rc<dyn Fn()>) {
        let mut slot = self.waiter.borrow_mut();
        if slot.as_ref().is_some_and(|cur| Rc::ptr_eq(cur, waiter)) {
            *slot = None;
        }
    }
}
impl Default for WakeTicket {
    fn default() -> Self {
        Self::new()
    }
}
pub trait Clock {
    fn now_ms(&self) -> u64;
    fn arms_timer(&self) -> bool {
        true
    }
}
/// 每轮 advance pin 住的时钟视图。
pub struct PinnedClock(u64);
impl Clock for PinnedClock {
    fn now_ms(&self) -> u64 {
        self.0
    }
    fn arms_timer(&self) -> bool {
        false
    }
}
pub fn pin_clock(now_ms: u64) -> PinnedClock {
    PinnedClock(now_ms)
}
/// 手动时钟：只在 advance(ms) 时前进，不装真实定时器。测试用。
pub struct ManualClock {
    now: Cell<u64>,
}
impl ManualClock {
    pub fn new(now_ms: u64) -> Self {
        Self {
            now: Cell::new(now_ms),
        }
    }
    pub fn advance(&self, ms: u64) {
        self.now.set(self.now.get().saturating_add(ms));
    }
}
impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.now.get()
    }
    fn arms_timer(&self) -> bool {
        false
    }
}
/// 单调时钟：读宿主时间。生产用。
#[cfg(feature = "std")]
pub struct MonotonicClock {
    start: std::time::Instant,
}
#[cfg(feature = "std")]
impl MonotonicClock {
    pub fn new() -> Self {
        Self {
            start: std::time::Instant::now(),
        }
    }
}
#[cfg(feature = "std")]
impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}
#[cfg(feature = "std")]
impl Clock for MonotonicClock {
    fn now_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
}
/// 定时堆只接受安全非负整数；NaN/负数/越界会污染堆根，使所有 timer 永不过期。
pub fn safe_deadline(value: u64) -> Result<u64> {
    // 拒绝 > 2^53-1 的极大 deadline。
    const MAX_SAFE_INTEGER: u64 = (1u64 << 53) - 1;
    if value > MAX_SAFE_INTEGER {
        return Err(EbaError::Clock("deadline_ms must be a safe integer".into()));
    }
    Ok(value)
}
/// 观测当前时钟（advance 开头 pin 住本轮 now）。
pub fn saturating_now(clock: &dyn Clock) -> u64 {
    clock.now_ms()
}
/// 堆元素：仅按 (at, seq) 排序，T 不参与 Ord —— 保持 T 的公开约束不变。
struct Key<T>(u64, u64, T);
impl<T> PartialEq for Key<T> {
    fn eq(&self, other: &Self) -> bool {
        (self.0, self.1) == (other.0, other.1)
    }
}
impl<T> Eq for Key<T> {}
impl<T> PartialOrd for Key<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<T> Ord for Key<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        (other.0, other.1).cmp(&(self.0, self.1)) // 反转：BinaryHeap 是最大堆
    }
}
/// 按 (at, 插入序) 排序的 deadline 最小堆。
pub struct Deadlines<T> {
    items: BinaryHeap<Key<T>>,
    seq: u64,
}
impl<T: Clone + PartialEq> Default for Deadlines<T> {
    fn default() -> Self {
        Self::new()
    }
}
impl<T: Clone + PartialEq> Deadlines<T> {
    pub fn new() -> Self {
        Self {
            items: BinaryHeap::new(),
            seq: 0,
        }
    }
    pub fn at(&self, id: &T) -> Option<u64> {
        self.items.iter().find(|k| k.2 == *id).map(|k| k.0)
    }
    pub fn next_ms(&self) -> Option<u64> {
        self.items.peek().map(|k| k.0)
    }
    /// 重复 id 抛普通 Error（可捕获），不是 panic。
    pub fn add(&mut self, id: T, at: u64) -> Result<()> {
        if self.at(&id).is_some() {
            return Err(EbaError::Other("duplicate deadline".into()));
        }
        self.seq += 1;
        self.items.push(Key(at, self.seq, id));
        Ok(())
    }
    pub fn remove(&mut self, id: &T) {
        self.items.retain(|k| k.2 != *id);
    }
    pub fn peek_due(&self, now_ms: u64) -> Option<T> {
        match self.items.peek() {
            Some(k) if k.0 <= now_ms => Some(k.2.clone()),
            _ => None,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}
/// 调度快照：告诉宿主是否有 ready work、下一次 deadline、是否已静止。
#[derive(Clone, Debug)]
pub struct Schedule {
    pub ready: bool,
    pub next_deadline_ms: Option<u64>,
    pub is_quiescent: bool,
    pub now_ms: Option<u64>,
}
impl Schedule {
    pub fn new(
        ready: bool,
        next_deadline_ms: Option<u64>,
        is_quiescent: bool,
        now_ms: Option<u64>,
    ) -> Self {
        Self {
            ready,
            next_deadline_ms,
            is_quiescent,
            now_ms,
        }
    }
}
/// 一次性唤醒句柄。no_std 下退化为标志位（宿主轮询 schedule 即可）。
pub struct Wake {
    fired: Cell<bool>,
}
impl Wake {
    pub fn new() -> Self {
        Self {
            fired: Cell::new(false),
        }
    }
    pub fn fire(&self) {
        self.fired.set(true);
    }
    pub fn consume(&self) -> bool {
        let v = self.fired.get();
        self.fired.set(false);
        v
    }
}
impl Default for Wake {
    fn default() -> Self {
        Self::new()
    }
}
