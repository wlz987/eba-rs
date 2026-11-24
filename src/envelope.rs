//! 信封、头、地址与结果。
use crate::errors::{EbaError, Result};
use crate::pattern::split_topic;
use crate::prelude::*;
use crate::value::Value;
use core::fmt;
pub type EnvelopeId = String;
pub type ActorId = String;
pub fn envelope_id(raw: impl Into<String>) -> EnvelopeId {
    raw.into()
}
pub fn actor_id(raw: impl Into<String>) -> ActorId {
    raw.into()
}
pub const MAX_PAYLOAD_BYTES: usize = 1024;
/// 是否为合法封装信封。
pub fn is_sealed(envelope: &Envelope) -> bool {
    envelope.is_sealed()
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Address {
    Event,
    Diagnostic,
    Request { reply_to: String },
    Result { correlation_id: EnvelopeId },
}
impl Address {
    pub fn kind(&self) -> &'static str {
        match self {
            Address::Event => "event",
            Address::Diagnostic => "diagnostic",
            Address::Request { .. } => "request",
            Address::Result { .. } => "result",
        }
    }
}
#[derive(Clone, Debug)]
pub struct Header {
    pub id: EnvelopeId,
    pub topic: String,
    pub from: ActorId,
    pub cause: Option<EnvelopeId>,
    pub address: Address,
    pub sv: u32,
    pub bytes: usize,
}
#[derive(Clone, Debug)]
pub struct Envelope {
    pub header: Header,
    pub payload: Value,
    sealed: bool,
}
impl Envelope {
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }
    pub fn with_payload(mut self, payload: Value) -> Self {
        self.payload = payload;
        self
    }
    pub fn with_schema_version(mut self, sv: u32) -> Self {
        self.header.sv = sv;
        self
    }
    /// 显式写入 payload 字节数。
    /// 失败返回 EnvelopeBuildError。
    pub fn check_trust(&self, policy: &crate::mail::TrustPolicy) -> Result<()> {
        if !policy.allows(&self.header.from) {
            return Err(EbaError::EnvelopeBuild("from is not trusted".into()));
        }
        Ok(())
    }
    /// 从头与 payload 重建信封（保留 sealing 状态）。
    pub fn from_parts(header: Header, payload: Value, sealed: bool) -> Self {
        Self {
            header,
            payload,
            sealed,
        }
    }
    pub fn correlation_id(&self) -> Option<&EnvelopeId> {
        match &self.header.address {
            Address::Result { correlation_id } => Some(correlation_id),
            _ => None,
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Completion {
    pub request_id: EnvelopeId,
    /// None 表示超时/无答案。
    pub body: Option<Value>,
}
impl Completion {
    pub fn is_timeout(&self) -> bool {
        self.body.is_none()
    }
}
pub trait IdGen {
    fn next(&self) -> EnvelopeId;
    /// 耗尽时抛 SeqExhausted（可捕获）；默认实现兼容 infallible 的自定义生成器。
    fn try_next(&self) -> Result<EnvelopeId> {
        Ok(self.next())
    }
}
/// 单调递增 id 生成器（Cell 提供内部可变，避免 &mut 约束）。
pub struct SeqIdGen {
    next: Cell<Option<u64>>,
}
impl SeqIdGen {
    pub fn new() -> Self {
        Self::with_start(1)
    }
    pub fn with_start(start: u64) -> Self {
        Self {
            next: Cell::new(Some(start)),
        }
    }
}
impl Default for SeqIdGen {
    fn default() -> Self {
        Self::new()
    }
}
impl IdGen for SeqIdGen {
    fn next(&self) -> EnvelopeId {
        self.try_next()
            .expect("SeqExhausted: envelope id space exhausted")
    }
    fn try_next(&self) -> Result<EnvelopeId> {
        let n = self
            .next
            .get()
            .ok_or_else(|| EbaError::SeqExhausted("envelope id space exhausted".into()))?;
        // 237：n = 2^53 仍发出最后一个 id，下次才耗尽。
        const MAX_SAFE: u64 = 1u64 << 53;
        let next = if n >= MAX_SAFE { None } else { Some(n + 1) };
        self.next.set(next);
        // : n.toString(16).padStart(32, "0")
        Ok(format!("{:032x}", n))
    }
}
#[cfg(feature = "std")]
pub struct UuidIdGen {
    counter: Cell<u64>,
}
#[cfg(feature = "std")]
impl UuidIdGen {
    pub fn new() -> Self {
        Self {
            counter: Cell::new(0),
        }
    }
}
#[cfg(feature = "std")]
impl Default for UuidIdGen {
    fn default() -> Self {
        Self::new()
    }
}
#[cfg(feature = "std")]
impl IdGen for UuidIdGen {
    fn next(&self) -> EnvelopeId {
        let n = self.counter.get();
        self.counter.set(n.wrapping_add(1));
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        // : randomUUID().replace(/-/g, "") —— 32 位 hex，无连字符。
        let value = nanos ^ ((n as u128) << 64) ^ 0x4000_0000_0000_0000_0000_0000_0000_0000u128;
        format!("{:032x}", value)
    }
}
pub struct EnvelopeBuildError(pub String);
impl EnvelopeBuildError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}
impl fmt::Display for EnvelopeBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
pub struct MakeEnvelope<'a> {
    pub topic: String,
    pub payload: Value,
    pub from: ActorId,
    pub id_gen: &'a dyn IdGen,
    pub address: Address,
    pub cause: Option<EnvelopeId>,
    pub sv: u32,
}
/// 构造并封装信封。topic 非法时返回 InvalidTopic。
fn make_envelope_inner(
    spec: MakeEnvelope<'_>,
    explicit_id: Option<EnvelopeId>,
) -> Result<Envelope> {
    if split_topic(&spec.topic).is_err() {
        return Err(EbaError::InvalidTopic(spec.topic.clone()));
    }
    let id = match explicit_id {
        Some(id) => id,
        None => spec.id_gen.try_next()?,
    };
    // cause 缺省等于自身 id（不是 None）。
    let cause = spec.cause.or_else(|| Some(id.clone()));
    Ok(Envelope {
        header: Header {
            id,
            topic: spec.topic,
            from: spec.from,
            cause,
            address: spec.address,
            sv: spec.sv,
            bytes: 0,
        },
        payload: spec.payload,
        sealed: true,
    })
}
pub fn make_envelope(spec: MakeEnvelope<'_>) -> Result<Envelope> {
    make_envelope_inner(spec, None)
}
/// 不消耗 idGen。
pub fn make_envelope_with_id(spec: MakeEnvelope<'_>, id: EnvelopeId) -> Result<Envelope> {
    make_envelope_inner(spec, Some(id))
}
