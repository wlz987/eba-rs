//! 错误层级。
use crate::prelude::*;
use core::fmt;
/// BusError / MailboxFull / InvalidTopic /
/// AmbiguousRoute / BackpressureError / DeliveryError / MailboxBusy / LeasePending /
/// WakeError / ClockError / RecordSinkError / SeqExhausted。
/// Rust 用枚举变体表达同一层级（另含 Closed/Reentrant/Other 内部态）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EbaError {
    Bus(String),
    MailboxFull(String),
    InvalidTopic(String),
    EnvelopeBuild(String),
    Pool(crate::pool::PoolError),
    InvalidCapability(String),
    AmbiguousRoute(String),
    Backpressure(String),
    Delivery(String),
    MailboxBusy(String),
    LeasePending(String),
    UnreadableResult(String),
    Wake(String),
    Clock(String),
    RecordSink(String),
    SeqExhausted(String),
    Closed(String),
    Reentrant(String),
    Other(String),
}
impl fmt::Display for EbaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let EbaError::Pool(error) = self {
            return write!(f, "{}: {}", error.class_name(), error);
        }
        let (name, msg) = match self {
            EbaError::Bus(m) => ("BusError", m),
            EbaError::MailboxFull(m) => ("MailboxFull", m),
            EbaError::InvalidTopic(m) => ("InvalidTopic", m),
            EbaError::EnvelopeBuild(m) => ("EnvelopeBuildError", m),
            EbaError::InvalidCapability(m) => ("InvalidCapability", m),
            EbaError::AmbiguousRoute(m) => ("AmbiguousRoute", m),
            EbaError::Backpressure(m) => ("BackpressureError", m),
            EbaError::Delivery(m) => ("DeliveryError", m),
            EbaError::MailboxBusy(m) => ("MailboxBusy", m),
            EbaError::LeasePending(m) => ("LeasePending", m),
            EbaError::UnreadableResult(m) => ("UnreadableResult", m),
            EbaError::Wake(m) => ("WakeError", m),
            EbaError::Clock(m) => ("ClockError", m),
            EbaError::RecordSink(m) => ("RecordSinkError", m),
            EbaError::SeqExhausted(m) => ("SeqExhausted", m),
            EbaError::Closed(m) => ("Closed", m),
            EbaError::Reentrant(m) => ("Reentrant", m),
            EbaError::Other(m) => ("Error", m),
            EbaError::Pool(_) => unreachable!("pool handled above"),
        };
        write!(f, "{}: {}", name, msg)
    }
}
#[cfg(feature = "std")]
impl std::error::Error for EbaError {}
/// 细粒度错误判定。
impl EbaError {
    pub fn is_mailbox_full(&self) -> bool {
        matches!(self, EbaError::MailboxFull(_))
    }
    pub fn is_ambiguous_route(&self) -> bool {
        matches!(self, EbaError::AmbiguousRoute(_))
    }
    pub fn is_clock(&self) -> bool {
        matches!(self, EbaError::Clock(_))
    }
    pub fn is_record_sink(&self) -> bool {
        matches!(self, EbaError::RecordSink(_))
    }
    pub fn is_backpressure(&self) -> bool {
        matches!(
            self,
            EbaError::Backpressure(_) | EbaError::MailboxBusy(_) | EbaError::MailboxFull(_)
        )
    }
    /// BusError/MailboxFull/InvalidTopic/AmbiguousRoute。
    pub fn is_bus_family(&self) -> bool {
        matches!(
            self,
            EbaError::Bus(_)
                | EbaError::MailboxFull(_)
                | EbaError::InvalidTopic(_)
                | EbaError::AmbiguousRoute(_)
        )
    }
    /// DeliveryError/MailboxBusy。
    pub fn is_delivery_family(&self) -> bool {
        matches!(self, EbaError::Delivery(_) | EbaError::MailboxBusy(_))
    }
    /// 严格背压判定（不含 MailboxBusy/MailboxFull）。
    pub fn is_backpressure_exact(&self) -> bool {
        matches!(self, EbaError::Backpressure(_))
    }
}
pub type Result<T> = core::result::Result<T, EbaError>;
/// 重入信号文本。
pub const REENTRANT: &str = "reentrant host execution";
/// 公共错误类型名。
/// Rust 用 `EbaError` 枚举表达同一层级；这些别名让调用方按 的名字引用。
pub type BusError = EbaError;
pub type MailboxFull = EbaError;
pub type InvalidTopic = EbaError;
pub type AmbiguousRoute = EbaError;
pub type BackpressureError = EbaError;
pub type DeliveryError = EbaError;
pub type MailboxBusy = EbaError;
pub type LeasePending = EbaError;
pub type WakeError = EbaError;
pub type ClockError = EbaError;
pub type RecordSinkError = EbaError;
pub type SeqExhausted = EbaError;
/// 具名错误的默认 message（构造器无参时的文本）。Rust 无默认参数重载，
/// 以常量暴露，生产路径必须复用而非自造文本。
pub const MAILBOX_BUSY: &str = "result mailbox full";
