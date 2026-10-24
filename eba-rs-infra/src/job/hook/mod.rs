use alloc::rc::Rc;

use super::delivery::defer_ending;
use super::ending::seal;
use super::execution::{commit_to, silence};
use crate::envelope::Envelope;
use crate::errors::{note_settled, BoxError};
use crate::job::core::{Core, HostState};
use crate::job::job::{seal_context, Context, Control, Job, SharedIdGen};
use crate::mail::bus::Bus;
use crate::mail::inbox::reply_topic;
use crate::time::clock::Clock;

mod deliver;
mod fail;

use self::fail::fail_job;

pub use self::deliver::deliver_ending;
pub(crate) use self::deliver::delivery_envelope;

pub fn run_hook<F>(
    core: &mut Core,
    bus: &Bus,
    clock: &dyn Clock,
    id_gen: &SharedIdGen,
    job: &Job,
    callback: F,
) -> Result<(), BoxError>
where
    F: FnOnce(&Context) -> Result<(), BoxError>,
{
    let root = job.root();
    let reply_to = reply_topic(&core.sub.inbox);
    let pending_count = core.book.count(&root.header.id);
    let ctx = Context::new(
        Rc::clone(&job.state),
        core.sub.actor_id.clone(),
        reply_to,
        Rc::clone(id_gen),
        pending_count,
    );

    let mut reply: Option<Rc<Envelope>> = None;
    let mut control: Option<Control> = None;
    let hook = callback(&ctx);
    let commit = seal_context(&ctx);
    let hold = commit.hold;
    let outcome = match hook {
        Ok(()) => (|| -> Result<(), BoxError> {
            let (sent_reply, sent_control) = commit_to(core, bus, clock, id_gen, job, commit)?;
            if hold {
                return Ok(());
            }
            reply = sent_reply;
            control = sent_control;
            if let Some(held) = control.take() {
                let settled = if reply.is_none() {
                    let (quiet, next) = silence(core, job, id_gen, held)?;
                    reply = quiet;
                    next
                } else {
                    held
                };
                control = Some(settled);
                seal(core, job)?;
            }
            Ok(())
        })(),
        Err(error) => Err(error),
    };

    if let Err(error) = outcome {
        if let Some(settled) = control.take() {
            let owed = if matches!(core.state, HostState::Running) {
                note_settled(error)
            } else {
                error
            };
            return Err(defer_ending(core, bus, job, settled, reply, owed));
        }
        return Err(note_settled(fail_job(core, bus, id_gen, job, error)));
    }

    deliver_ending(core, bus, job, reply, control)
}
