use crate::prelude::*;

use crate::errors::{BoxError, BusError};
use crate::job::core::Core;
use crate::job::effect::Effect;

/// Apply the effects a finished Job may still emit (no asks/holds/replies).
pub fn apply_finished(core: &mut Core, effects: &[Effect]) -> Result<(), BoxError> {
    for effect in effects {
        match effect {
            Effect::Close { .. } => core.begin_close(),
            Effect::Settle(lease, body) => lease.send(body.clone())?,
            Effect::Revoke(lease, error) => {
                lease.revoke(error.clone())?;
            }
            Effect::Tell { topic, payload } => host(core)?.tell(topic, payload.clone())?,
            Effect::Record { topic, payload } => host(core)?.record(topic, payload.clone())?,
            Effect::Barrier { up_to } => host(core)?.barrier(*up_to)?,
            Effect::Timer { deadline_ms } => host(core)?.timer(*deadline_ms)?,
            _ => {
                return Err(Box::from(BusError("unsupported onFinished effect".into())));
            }
        }
    }
    Ok(())
}

fn host(core: &Core) -> Result<alloc::rc::Rc<dyn crate::job::effect::EffectHost>, BoxError> {
    core.host
        .clone()
        .ok_or_else(|| Box::from(BusError("Effect host is not attached".into())))
}
