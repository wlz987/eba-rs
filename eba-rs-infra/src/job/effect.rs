use crate::envelope::result::ResultBody;
use crate::errors::{BoxError, BusError};
use crate::job::context::Context;
use crate::Payload;
use alloc::boxed::Box;
use alloc::string::String;

pub trait EffectHost {
    fn tell(&self, topic: &str, payload: Payload) -> Result<(), BoxError>;
    fn record(&self, topic: &str, payload: Payload) -> Result<(), BoxError>;
    fn barrier(&self, up_to: Option<u64>) -> Result<(), BoxError>;
    fn timer(&self, deadline_ms: u64) -> Result<(), BoxError>;
    fn close(&self, reason: Option<String>) -> Result<(), BoxError>;
}

/// A declarative job decision; the runtime applies it in order.
#[derive(Clone)]
pub enum Effect {
    Ask { intent: crate::job::job::Intent },
    Reply(ResultBody),
    Hold(crate::job::JobLease),
    Finish(Option<ResultBody>),
    Settle(crate::job::JobLease, ResultBody),
    Revoke(crate::job::JobLease, String),
    Tell { topic: String, payload: Payload },
    Record { topic: String, payload: Payload },
    Barrier { up_to: Option<u64> },
    Timer { deadline_ms: u64 },
    Close { reason: Option<String> },
}

pub fn apply_effects(ctx: &Context, effects: &[Effect]) -> Result<(), BoxError> {
    for (index, effect) in effects.iter().enumerate() {
        if matches!(effect, Effect::Barrier { .. } | Effect::Timer { .. })
            && index + 1 < effects.len()
        {
            return Err(Box::from(BusError(
                "barrier/timer must be the last effect".into(),
            )));
        }
        effect.apply(ctx)?;
    }
    Ok(())
}

impl Effect {
    pub fn apply(&self, ctx: &Context) -> Result<(), BoxError> {
        match self {
            Effect::Ask { intent } => ctx.push_intent(intent.clone()),
            Effect::Reply(body) => ctx.apply_reply(body.clone()),
            Effect::Hold(lease) => ctx.apply_hold(lease.clone()),
            Effect::Finish(body) => ctx.apply_finish(body.clone()),
            Effect::Settle(lease, body) => {
                lease.send(body.clone())?;
                Ok(())
            }
            Effect::Revoke(lease, error) => {
                lease.revoke(error.clone())?;
                Ok(())
            }
            Effect::Tell { topic, payload } => ctx.effect_host()?.tell(topic, payload.clone()),
            Effect::Record { topic, payload } => ctx.effect_host()?.record(topic, payload.clone()),
            Effect::Barrier { up_to } => ctx.effect_host()?.barrier(*up_to),
            Effect::Timer { deadline_ms } => ctx.effect_host()?.timer(*deadline_ms),
            Effect::Close { reason } => ctx.effect_host()?.close(reason.clone()),
        }
    }
}
