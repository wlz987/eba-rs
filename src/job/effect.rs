//! Effect 联合类型（11 支）。
//! Rust 的 match 天然穷尽，强于 never 臂。
use crate::job::lease::Lease;
use crate::prelude::*;
use crate::value::Value;
#[derive(Clone)]
pub struct Intent {
    pub stage: String,
    pub request: crate::envelope::Envelope,
    pub timeout_ms: Option<u64>,
    pub reply_to: String,
}
#[derive(Clone)]
pub enum Effect {
    Ask { intent: Intent },
    Reply { body: Value },
    Hold { lease: Lease },
    Finish { body: Value },
    Settle { lease: Lease, body: Value },
    Revoke { lease: Lease, error: Option<String> },
    Tell { topic: String, payload: Value },
    Record { topic: String, payload: Value },
    Barrier { up_to: Option<u64> },
    Timer { deadline_ms: u64 },
    Close { reason: Option<String> },
}
impl Effect {
    pub fn kind(&self) -> &'static str {
        match self {
            Effect::Ask { .. } => "ask",
            Effect::Reply { .. } => "reply",
            Effect::Hold { .. } => "hold",
            Effect::Finish { .. } => "finish",
            Effect::Settle { .. } => "settle",
            Effect::Revoke { .. } => "revoke",
            Effect::Tell { .. } => "tell",
            Effect::Record { .. } => "record",
            Effect::Barrier { .. } => "barrier",
            Effect::Timer { .. } => "timer",
            Effect::Close { .. } => "close",
        }
    }
}
/// 把 hook 的返回值归一为 Effect 数组。
pub fn to_effect_array(value: Option<Vec<Effect>>) -> Vec<Effect> {
    value.unwrap_or_default()
}
