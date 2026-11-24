//! LeaseRegistry：回信槽与到期记账。
//!
//! 到期未答的租约必须把 requestId 排入取消队列，否则下游 worker 的 job 永不结束、
//! 宿主永不 quiescent（这是 I2 bug 的教训）。
use crate::envelope::{Completion, Envelope, EnvelopeId};
use crate::errors::{EbaError, Result};
use crate::job::lease::Lease;
use crate::prelude::*;
use crate::time::Deadlines;
use crate::value::Value;
pub struct LeaseContinuation {
    pub owner: EnvelopeId,
    pub stage: String,
}
struct LeaseEntry {
    lease: Lease,
    continuation: LeaseContinuation,
    /// 回信必须匹配原请求的 topic 与 cause。
    topic: String,
    cause: Option<EnvelopeId>,
}
pub struct LeaseRegistry {
    entries: BTreeMap<EnvelopeId, LeaseEntry>,
    deadlines: Deadlines<EnvelopeId>,
    owners: BTreeMap<EnvelopeId, BTreeSet<EnvelopeId>>,
    cancelled: Vec<EnvelopeId>,
    /// LeaseRecords.stuck：关闭订阅失败时记住 requestId，等待 recover 重试。
    stuck: BTreeSet<EnvelopeId>,
}
impl LeaseRegistry {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            deadlines: Deadlines::new(),
            owners: BTreeMap::new(),
            cancelled: Vec::new(),
            stuck: BTreeSet::new(),
        }
    }
    pub fn size(&self) -> usize {
        self.entries.len()
    }
    pub fn next_deadline_ms(&self) -> Option<u64> {
        self.deadlines.next_ms()
    }
    pub fn holds(&self, request_id: &EnvelopeId) -> bool {
        self.entries.contains_key(request_id)
    }
    pub fn count(&self, owner: &EnvelopeId) -> usize {
        self.owners.get(owner).map(|s| s.len()).unwrap_or(0)
    }
    #[allow(clippy::too_many_arguments)]
    pub fn submit(
        &mut self,
        request_id: EnvelopeId,
        lease: Lease,
        owner: EnvelopeId,
        stage: impl Into<String>,
        deadline_ms: Option<u64>,
        topic: impl Into<String>,
        cause: Option<EnvelopeId>,
    ) -> Result<()> {
        // 重复 request id 抛 EnvelopeBuildError，
        // 不能静默覆盖，也不能 panic（panic=abort 下不可恢复）。
        if self.entries.contains_key(&request_id) {
            return Err(EbaError::EnvelopeBuild(format!(
                "duplicate request id: {}",
                request_id
            )));
        }
        let continuation = LeaseContinuation {
            owner: owner.clone(),
            stage: stage.into(),
        };
        self.entries.insert(
            request_id.clone(),
            LeaseEntry {
                lease: lease.clone(),
                continuation,
                topic: topic.into(),
                cause,
            },
        );
        self.owners
            .entry(owner)
            .or_default()
            .insert(request_id.clone());
        if let Some(d) = deadline_ms {
            let _ = self.deadlines.add(request_id, d);
        }
        Ok(())
    }
    /// 回信到达：取走 body，返回 (owner, stage, completion)。
    pub fn take_result(
        &mut self,
        envelope: &Envelope,
    ) -> Result<Option<(EnvelopeId, String, Completion)>> {
        let Some(id) = envelope.correlation_id().cloned() else {
            return Ok(None);
        };
        // 169-178：回信的 topic/cause 必须与
        // 原请求登记的一致；不符视为无主回信，返回 null（不移除租约）。
        if !self
            .entries
            .get(&id)
            .is_some_and(|e| e.topic == envelope.header.topic && e.cause == envelope.header.cause)
        {
            return Ok(None);
        }
        // resultBody 失败或 body=null 抛 UnreadableResult，
        // 而非返回 null。租约保持登记，由 resume 的 catch 归入 abandoned。
        let body = {
            let Some(entry) = self.entries.get(&id) else {
                return Ok(None);
            };
            match entry.lease.take() {
                Some(body) => body,
                None if envelope.payload == Value::Null => {
                    return Err(EbaError::UnreadableResult("result body is null".into()));
                }
                None => envelope.payload.clone(),
            }
        };
        let Some(entry) = self.entries.remove(&id) else {
            return Ok(None);
        };
        self.deadlines.remove(&id);
        self.forget(&entry.continuation.owner, &id);
        let completion = Completion {
            request_id: id.clone(),
            body: Some(body),
        };
        Ok(Some((
            entry.continuation.owner,
            entry.continuation.stage,
            completion,
        )))
    }
    /// 到期：返回 (owner, stage, completion=None 表示超时)，并排入取消队列。
    pub fn expire_due(&mut self, now_ms: u64) -> Option<(EnvelopeId, String, Completion)> {
        let id = self.deadlines.peek_due(now_ms)?;
        self.deadlines.remove(&id);
        let entry = self.entries.remove(&id)?;
        self.forget(&entry.continuation.owner, &id);
        // 无人应答：下游 worker 的 job 需要被 revoke，排入取消队列。
        self.cancelled.push(id.clone());
        Some((
            entry.continuation.owner,
            entry.continuation.stage,
            Completion {
                request_id: id,
                body: None,
            },
        ))
    }
    /// 快速判空：供 Runtime 空转时短路整段取消收集（零分配、零 RefCell 操作）。
    pub fn has_cancelled(&self) -> bool {
        !self.cancelled.is_empty()
    }
    pub fn take_cancelled(&mut self) -> Vec<EnvelopeId> {
        core::mem::take(&mut self.cancelled)
    }
    /// 按 requestId 主动结算租约（bury 后释放等待者），返回 (owner, stage)。
    pub fn take_by_id(&mut self, request_id: &EnvelopeId) -> Option<(EnvelopeId, String)> {
        let entry = self.entries.remove(request_id)?;
        self.deadlines.remove(request_id);
        self.forget(&entry.continuation.owner, request_id);
        Some((entry.continuation.owner, entry.continuation.stage))
    }
    pub fn cancel(&mut self, owner: &EnvelopeId) {
        if let Some(ids) = self.owners.remove(owner) {
            for id in ids {
                if let Some(entry) = self.entries.remove(&id) {
                    let _ = entry.lease.revoke("cancelled");
                    self.deadlines.remove(&id);
                    self.cancelled.push(id);
                }
            }
        }
    }
    /// 级联取消（V9 / LYR5）：取消 owner 后，以其下层 request_id 为 owner 的
    /// Lease 也递归失效——三层并发中 correlationId 沿原路贯穿。
    pub fn cancel_cascade(&mut self, owner: &EnvelopeId) {
        self.cancel_cascade_inner(owner, 0);
    }
    fn cancel_cascade_inner(&mut self, owner: &EnvelopeId, depth: usize) {
        // 有界递归：防止异常深的 owner 链导致栈溢出（drainCancelledAll 也有 256 轮上限）。
        if depth >= 256 {
            return;
        }
        let ids = self.owners.remove(owner).unwrap_or_default();
        for id in ids {
            if let Some(entry) = self.entries.remove(&id) {
                let _ = entry.lease.revoke("cancelled");
                self.deadlines.remove(&id);
                self.cancelled.push(id.clone());
                // 下层 Lease 的 owner 是本次被取消的 request_id，递归。
                self.cancel_cascade_inner(&id, depth + 1);
            }
        }
    }
    /// LeaseRecords.remember：订阅关闭失败时记住 requestId 供 recover 重试。
    pub fn remember_stuck(&mut self, id: EnvelopeId) {
        self.stuck.insert(id);
    }
    pub fn unreleased(&self) -> usize {
        self.stuck.len()
    }
    /// LeaseRecords.recover：重试释放 stuck 租约并清理账簿。
    /// 同步模型下没有异步 close 失败，这里按 id 结算并返回清理数量。
    pub fn recover(&mut self) -> usize {
        let ids: Vec<EnvelopeId> = core::mem::take(&mut self.stuck).into_iter().collect();
        for id in &ids {
            let _ = self.take_by_id(id);
        }
        ids.len()
    }
    /// LeaseRecords.elapseOpen：无 deadline 的 open 请求挂 ELAPSED=0 立即到期。
    pub fn elapse_open(&mut self) -> usize {
        let ids: Vec<EnvelopeId> = self
            .entries
            .keys()
            .filter(|id| self.deadlines.at(id).is_none())
            .cloned()
            .collect();
        for id in &ids {
            let _ = self.deadlines.add(id.clone(), 0);
        }
        ids.len()
    }
    fn forget(&mut self, owner: &EnvelopeId, id: &EnvelopeId) {
        if let Some(set) = self.owners.get_mut(owner) {
            set.remove(id);
            if set.is_empty() {
                self.owners.remove(owner);
            }
        }
    }
}
impl Default for LeaseRegistry {
    fn default() -> Self {
        Self::new()
    }
}
