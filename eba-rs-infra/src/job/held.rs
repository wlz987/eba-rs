use alloc::rc::Rc;

use crate::envelope::result::ResultBody;
use crate::envelope::Envelope;
use crate::errors::BoxError;
use crate::job::core::Core;
use crate::job::delivery::match_reply;
use crate::job::ending::seal;
use crate::job::hook::deliver_ending;
use crate::job::job::{Control, Job, SharedIdGen};
use crate::mail::bus::Bus;

/// Settle held replies whose slot has a body; called at the end of a runtime turn.
pub(crate) fn resolve_held(
    core: &mut Core,
    bus: &Bus,
    id_gen: &SharedIdGen,
) -> Result<bool, BoxError> {
    let mut progressed = false;
    loop {
        let mut ready: Option<(Job, Rc<Envelope>, ResultBody)> = None;
        for job in core.jobs.values() {
            let state = job.state.borrow();
            if !state.held {
                continue;
            }
            let Some(slot) = &state.reply_slot else {
                continue;
            };
            let Some(body) = slot.take() else {
                continue;
            };
            ready = Some((job.clone(), state.root.clone(), body));
            break;
        }
        let Some((job, root, body)) = ready else {
            break;
        };
        {
            let mut state = job.state.borrow_mut();
            state.held = false;
            state.reply_slot = None;
        }
        let result = match_reply(core, id_gen, &root, &body)?;
        seal(core, &job)?;
        deliver_ending(
            core,
            bus,
            &job,
            Some(Rc::new(result)),
            Some(Control { body: Some(body) }),
        )?;
        progressed = true;
    }
    Ok(progressed)
}

pub(crate) fn close_held(core: &mut Core) {
    for job in core.jobs.values() {
        let mut state = job.state.borrow_mut();
        if let Some(slot) = state.reply_slot.take() {
            slot.close();
        }
        state.held = false;
    }
}
