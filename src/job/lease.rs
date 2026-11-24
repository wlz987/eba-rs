//! Lease：等待凭据。
//!
//! owner 用 root envelope id 比较（代替 WeakRef），settle/send 按值拷贝 body
//! 防止交付后被外部改写。
use crate::errors::Result;
use crate::prelude::*;
use crate::value::{err_payload, has_result_shape, Value};
#[derive(Clone, Default)]
pub struct LeaseOptions {
    pub deadline_ms: Option<u64>,
    pub budget_ms: Option<u64>,
}
struct LeaseState {
    body: Option<Value>,
    closed: bool,
    aborted: bool,
    deadline_ms: Option<u64>,
    budget_ms: Option<u64>,
    spent: u64,
    owner: Option<String>,
}
/// 可共享、内部可变的 Lease 句柄。
#[derive(Clone)]
pub struct Lease {
    state: Rc<RefCell<LeaseState>>,
}
impl Lease {
    pub fn new(options: LeaseOptions) -> Self {
        Self {
            state: Rc::new(RefCell::new(LeaseState {
                body: None,
                closed: false,
                aborted: false,
                deadline_ms: options.deadline_ms,
                budget_ms: options.budget_ms,
                spent: 0,
                owner: None,
            })),
        }
    }
    pub fn bind(&self, owner: impl Into<String>) {
        self.state.borrow_mut().owner = Some(owner.into());
    }
    pub fn is_bound_to(&self, owner: &str) -> bool {
        self.state.borrow().owner.as_deref() == Some(owner)
    }
    pub fn is_bound(&self) -> bool {
        self.state.borrow().owner.is_some()
    }
    pub fn deadline_ms(&self) -> Option<u64> {
        self.state.borrow().deadline_ms
    }
    /// remainingMs 是预算余量（budgetMs - spent），不是距 deadline 的墙钟。
    /// 超预算返回负数，不做饱和钳制。
    pub fn remaining_ms(&self, _now: u64) -> Option<i64> {
        let s = self.state.borrow();
        s.budget_ms.map(|b| b as i64 - s.spent as i64)
    }
    pub fn expired(&self, now: u64) -> bool {
        let s = self.state.borrow();
        // 只比 deadline，不看 settled/closed（调用方自行跳过 closed）。
        s.deadline_ms.map(|d| d <= now).unwrap_or(false)
    }
    pub fn consume(&self, ms: u64) -> bool {
        let mut s = self.state.borrow_mut();
        match s.budget_ms {
            Some(budget) => {
                s.spent = s.spent.saturating_add(ms);
                s.spent <= budget
            }
            None => true,
        }
    }
    pub fn is_aborted(&self) -> bool {
        self.state.borrow().aborted
    }
    pub fn is_settled(&self) -> bool {
        self.state.borrow().body.is_some()
    }
    pub fn is_closed(&self) -> bool {
        self.state.borrow().closed
    }
    pub fn settle(&self, body: Value) -> Result<()> {
        self.send(body)
    }
    pub fn send(&self, body: Value) -> Result<()> {
        let mut s = self.state.borrow_mut();
        if s.closed {
            return Err(crate::errors::EbaError::LeasePending("lease closed".into()));
        }
        if s.aborted {
            return Ok(());
        }
        if s.body.is_some() {
            return Err(crate::errors::EbaError::LeasePending(
                "lease already settled".into(),
            ));
        }
        if !has_result_shape(&body) {
            return Err(crate::errors::EbaError::Other("invalid result body".into()));
        }
        // 按值拷贝：调用方的 live object 在交付后仍可能被改写。
        s.body = Some(body.clone());
        Ok(())
    }
    pub fn fail(&self, error: impl Into<String>) -> Result<()> {
        self.send(err_payload(error))
    }
    pub fn revoke(&self, error: impl Into<String>) -> bool {
        self.cancel(error)
    }
    /// 取消即写入 errPayload body 再 abort。
    /// 只有写 body 才能让 held 的 revoke 被 resolve_held 取回并结算 onFinished
    /// （否则 held job 永不结束，I2 断裂）。
    pub fn cancel(&self, error: impl Into<String>) -> bool {
        let mut s = self.state.borrow_mut();
        // 只检查 body/closed，不检查 aborted——
        // abort 只置信号，之后 cancel 仍可写入 errPayload body（撤销闭环需要）。
        if s.closed || s.body.is_some() {
            return false;
        }
        s.body = Some(err_payload(error));
        s.aborted = true;
        true
    }
    /// 只置 aborted，不写 body（cancel 才会写），且无 closed 检查。
    pub fn abort(&self) -> bool {
        let mut s = self.state.borrow_mut();
        if s.aborted {
            return false;
        }
        s.aborted = true;
        true
    }
    /// 返回 body 但不清空（is_settled 仍为 true）。
    pub fn take(&self) -> Option<Value> {
        self.state.borrow().body.clone()
    }
    pub fn peek(&self) -> Option<Value> {
        self.state.borrow().body.clone()
    }
    pub fn close(&self) {
        self.state.borrow_mut().closed = true;
    }
}
