//! Mailbox 出站背压状态机：park → rearm → bury。
//!
//! 信封身份是 **id**，不是对象引用：rearm/bury 都按 id 比较，否则两个同 id
//! 的对象会误伤彼此的 parked 槽。
use crate::envelope::Envelope;
use crate::errors::{EbaError, Result};
use crate::mail::Bus;
use crate::prelude::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    Idle,
    Checked,
    Advanced,
}
/// 按 topic 分组的 parked 信。同 topic 内是 FIFO。
struct Group {
    items: Vec<Envelope>,
}
/// 有界出站邮箱。满时不丢信：park 到组里，DELIVERY 相重试；两次失败 bury。
pub struct Mailbox {
    limit: usize,
    count: usize,
    groups: BTreeMap<String, Group>,
    /// 插入序，delete 后重设移到末尾。
    candidates: Vec<String>,
    /// 二次失败埋葬的信，由 Core 取走结算 delivery_failed。
    buried: Vec<Envelope>,
}
impl Mailbox {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            count: 0,
            groups: BTreeMap::new(),
            candidates: Vec::new(),
            buried: Vec::new(),
        }
    }
    /// 已存在的键不移动顺序，新键追加到末尾。
    fn mark_candidate(&mut self, topic: String) {
        if !self.candidates.contains(&topic) {
            self.candidates.push(topic);
        }
    }
    fn unmark_candidate(&mut self, topic: &str) {
        if let Some(pos) = self.candidates.iter().position(|t| t == topic) {
            self.candidates.remove(pos);
        }
    }
    pub fn len(&self) -> usize {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    pub fn has_room(&self) -> bool {
        self.count < self.limit
    }
    fn is_head(group: &Group, envelope: &Envelope) -> bool {
        group.items.first().map(|e| &e.header.id) == Some(&envelope.header.id)
    }
    /// 直接投递，满则 park（不丢信）。返回是否真正送达（Ok(true)）。
    /// published==false 且无 deadline 时调用方要立即置 due=ELAPSED。
    pub fn send(&mut self, bus: &Bus, envelope: Envelope) -> Result<bool> {
        // 19-43：目标 topic 已有 parked 组时一律追加组尾，
        // 不再直投，否则后到的信可能越过更早的 parked 信（破坏同 topic FIFO）。
        if self.groups.contains_key(&envelope.header.topic) {
            self.park(envelope)?;
            return Ok(true);
        }
        match bus.publish(&envelope) {
            // 37-40：published===false 且该回信被 awaited 时抛 DeliveryError，
            // 让调用方结算 delivery_failed；非 awaited 时静默返回 false。
            Ok(false) if bus.waiting(envelope.correlation_id().map(|s| s.as_str())) => {
                Err(EbaError::Delivery("result publication failed".into()))
            }
            Ok(delivered) => Ok(delivered),
            Err(EbaError::MailboxFull(_)) => {
                self.park(envelope)?;
                Ok(true) // parked，稍后重试，不算"无路由"
            }
            // 硬错误统一包装为 DeliveryError，
            // 调用方（含 settle_reply）看到的是具名投递错误而非底层 trust/route 错误。
            // 29-34：message 精确为 "result publication failed"，
            // 底层错误只作 cause（Rust 无 cause 字段，不拼进 message）。
            Err(_) => Err(EbaError::Delivery("result publication failed".into())),
        }
    }
    fn park(&mut self, envelope: Envelope) -> Result<()> {
        if !self.has_room() {
            // new MailboxBusy(envelope) 的默认 message 为 result mailbox full。
            return Err(EbaError::MailboxBusy(crate::errors::MAILBOX_BUSY.into()));
        }
        let topic = envelope.header.topic.clone();
        let first = !self.groups.contains_key(&topic);
        self.groups
            .entry(topic.clone())
            .or_insert(Group { items: Vec::new() })
            .items
            .push(envelope);
        self.count += 1;
        if first {
            self.mark_candidate(topic);
        }
        Ok(())
    }
    /// 空间腾出后重新排队：只有仍是组头才 rearm（防止陈旧标记误投）。
    pub fn rearm(&mut self, envelope: &Envelope) {
        let topic = &envelope.header.topic;
        // 仅有资格（组头）才登记 candidate；陈旧标记需清除，
        // 否则非组头留下 candidate 会造成空转 step。
        if self
            .groups
            .get(topic)
            .is_some_and(|group| Self::is_head(group, envelope))
        {
            self.mark_candidate(topic.clone());
        } else {
            self.unmark_candidate(topic);
        }
    }
    /// 两次失败后放弃该信，返回它以便调用方结算 delivery_failed。
    pub fn bury(&mut self, envelope: &Envelope) -> Option<Envelope> {
        let topic = &envelope.header.topic;
        let buried = {
            let group = self.groups.get_mut(topic)?;
            if !Self::is_head(group, envelope) {
                return None;
            }
            group.items.remove(0)
        };
        self.count = self.count.saturating_sub(1);
        self.unmark_candidate(topic);
        // 49-55：bury 组头后若组内仍有信，重新登记 candidate，
        // 否则同 topic 的后续信永久滞留（原实现只在空组时 remove，丢候选）。
        let group_empty = self
            .groups
            .get(topic)
            .map(|g| g.items.is_empty())
            .unwrap_or(true);
        if group_empty {
            self.groups.remove(topic);
        } else {
            self.mark_candidate(topic.clone());
        }
        Some(buried)
    }
    /// DELIVERY 相：重试一个候选信。
    /// 111：第二个返回值是成功出队的信封（sent）；
    /// 硬错误/awaited 失败的信由内部 buried 收集，交 Core 结算 delivery_failed。
    pub fn flush_one(&mut self, bus: &Bus) -> (Step, Option<Envelope>) {
        let Some(topic) = self.candidates.first().cloned() else {
            return (Step::Idle, None);
        };
        self.unmark_candidate(&topic);
        // 标记有候选但组缺失是可恢复状态，不是崩溃：仍返回 step。
        let Some(envelope) = self
            .groups
            .get(&topic)
            .and_then(|g| g.items.first().cloned())
        else {
            return (Step::Idle, None);
        };
        match bus.publish(&envelope) {
            Ok(true) => {
                self.groups.get_mut(&topic).unwrap().items.remove(0);
                self.count = self.count.saturating_sub(1);
                if self
                    .groups
                    .get(&topic)
                    .map(|g| g.items.is_empty())
                    .unwrap_or(false)
                {
                    self.groups.remove(&topic);
                } else {
                    self.mark_candidate(topic);
                }
                (Step::Advanced, Some(envelope))
            }
            Ok(false) => {
                // 97-100：只有被 awaited 的回信才算投递失败；
                // 非 awaited（普通 event/无等待者）正常出队，不当死信。
                if bus.waiting(envelope.correlation_id().map(|s| s.as_str())) {
                    if let Some(letter) = self.bury(&envelope) {
                        self.buried.push(letter);
                    }
                    (Step::Checked, None)
                } else {
                    self.groups.get_mut(&topic).unwrap().items.remove(0);
                    self.count = self.count.saturating_sub(1);
                    let empty = self
                        .groups
                        .get(&topic)
                        .map(|g| g.items.is_empty())
                        .unwrap_or(false);
                    if empty {
                        self.groups.remove(&topic);
                    } else {
                        self.mark_candidate(topic);
                    }
                    (Step::Advanced, Some(envelope))
                }
            }
            Err(EbaError::MailboxFull(_)) => {
                // 84-91：满只重新登记候选并等空间，不计数、不 bury。
                self.mark_candidate(topic);
                (Step::Checked, None)
            }
            Err(_) => {
                if let Some(letter) = self.bury(&envelope) {
                    self.buried.push(letter);
                }
                (Step::Checked, None)
            }
        }
    }
    /// 取走本轮被埋的信，交 Core 结算 delivery_failed。
    pub fn take_buried(&mut self) -> Vec<Envelope> {
        core::mem::take(&mut self.buried)
    }
    /// close：把所有 parked 信取出（交还调用方重新发布）。
    pub fn drain(&mut self) -> Vec<Envelope> {
        let mut out = Vec::new();
        for group in self.groups.values_mut() {
            out.append(&mut group.items);
        }
        self.groups.clear();
        self.candidates.clear();
        self.count = 0;
        out.append(&mut self.buried);
        out
    }
}
