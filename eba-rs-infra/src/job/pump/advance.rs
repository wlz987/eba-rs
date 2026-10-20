use crate::errors::BoxError;
use crate::job::core::{Core, HostState};
use crate::job::ending::release_deferred;
use crate::job::job::SharedIdGen;
use crate::mail::bus::Bus;
use crate::notify::signal::READ;
use crate::time::clock::Clock;

use super::step::pump;
use super::{enter, holding, idle, refresh};

pub fn advance_host(
    core: &mut Core,
    bus: &Bus,
    clock: &dyn Clock,
    id_gen: &SharedIdGen,
) -> Result<bool, BoxError> {
    let now = clock.now_ms();
    enter(core, false)?;
    let outcome = match release_deferred(core) {
        Ok(released) => match pump(core, bus, clock, id_gen, core.work_budget) {
            Err(error) => Err(error),
            Ok(progressed) => {
                let held = core.book.unreleased();
                core.book.recover();
                Ok(progressed || released || core.book.unreleased() < held)
            }
        },
        Err(error) => Err(error),
    };
    idle(core);
    refresh(core, now);
    outcome
}

pub fn has_work(core: &Core) -> bool {
    core.state == HostState::Closing
        || !core.jobs.is_empty()
        || core.queue.size() > 0
        || core.book.size() > 0
        || core.outbox.size() > 0
        || holding(core)
        || core.book.unreleased() > 0
        || core.signal.has(READ)
        || core
            .abandoned
            .as_ref()
            .is_some_and(|items| !items.is_empty())
}
