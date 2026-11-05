use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::vec::Vec;
use core::cell::{Cell, RefCell};

use super::delivery::defer_ending;
use super::ending::seal;
use super::execution::{commit_to, silence};
use crate::envelope::ids::ActorId;
use crate::envelope::{make_envelope, Envelope, MakeEnvelope};
use crate::errors::{note_settled, BoxError, BusError};
use crate::job::core::{Core, HostState};
use crate::job::effect::{apply_effects, Effect, EffectHost};
use crate::job::job::{seal_context, Context, Control, Job, SharedIdGen};
use crate::mail::bus::Bus;
use crate::mail::inbox::reply_topic;
use crate::record::RecordLog;
use crate::time::clock::Clock;

mod deliver;
mod fail;

use self::fail::fail_job;

pub use self::deliver::deliver_ending;
pub(crate) use self::deliver::delivery_envelope;

struct TellHost {
    bus: Bus,
    actor: ActorId,
    id_gen: SharedIdGen,
    records: Rc<RefCell<RecordLog>>,
    at_ms: u64,
    close: Rc<Cell<bool>>,
    timers: Rc<RefCell<Vec<u64>>>,
}

impl EffectHost for TellHost {
    fn tell(&self, topic: &str, payload: crate::Payload) -> Result<(), BoxError> {
        let id = self.id_gen.borrow_mut().next_envelope_id();
        let envelope = make_envelope(MakeEnvelope {
            topic: topic.into(),
            payload,
            from: self.actor.clone(),
            cause: None,
            id: Some(id),
            kind: "event".into(),
            reply_to: None,
            correlation_id: None,
        })
        .map_err(|error| Box::new(error) as BoxError)?;
        self.bus.publish_best_effort(&Rc::new(envelope)).map(|_| ())
    }

    fn record(&self, topic: &str, payload: crate::Payload) -> Result<(), BoxError> {
        let id = self.id_gen.borrow_mut().next_envelope_id();
        let envelope = make_envelope(MakeEnvelope {
            topic: topic.into(),
            payload,
            from: self.actor.clone(),
            cause: None,
            id: Some(id),
            kind: "event".into(),
            reply_to: None,
            correlation_id: None,
        })
        .map_err(|error| Box::new(error) as BoxError)?;
        self.records
            .borrow_mut()
            .record(Rc::new(envelope), self.at_ms)
            .map(|_| ())
            .ok_or_else(|| Box::from(BusError("record ring full".into())))
    }

    fn barrier(&self, up_to: Option<u64>) -> Result<(), BoxError> {
        // The actual deferral is scheduled by `run_hook` via `core.defer_barrier`
        // using the `wait_spec` derived from `Effect::Barrier`. This call only
        // validates the seq and records intent; it does not itself defer.
        let _seq = up_to.unwrap_or_else(|| self.records.borrow().next_seq().saturating_sub(1));
        Ok(())
    }
    fn timer(&self, deadline_ms: u64) -> Result<(), BoxError> {
        self.timers.borrow_mut().push(deadline_ms);
        Ok(())
    }
    fn close(&self, _reason: Option<alloc::string::String>) -> Result<(), BoxError> {
        self.close.set(true);
        Ok(())
    }
}

pub type HookCallback = Box<dyn FnOnce(&Context) -> Result<Vec<Effect>, BoxError>>;

pub fn run_hook(
    core: &mut Core,
    bus: &Bus,
    clock: &dyn Clock,
    id_gen: &SharedIdGen,
    job: &Job,
    callback: HookCallback,
) -> Result<(), BoxError> {
    let root = job.root();
    let reply_to = reply_topic(&core.sub.inbox);
    let pending_count = core.book.count(&root.header.id);
    let close = Rc::new(Cell::new(false));
    let ctx = Context::new(
        Rc::clone(&job.state),
        core.sub.actor_id.clone(),
        reply_to,
        Rc::clone(id_gen),
        pending_count,
    );
    let host = Rc::new(TellHost {
        bus: bus.clone(),
        actor: core.sub.actor_id.clone(),
        id_gen: Rc::clone(id_gen),
        records: Rc::clone(&core.records),
        at_ms: clock.now_ms(),
        close: Rc::clone(&close),
        timers: Rc::clone(&core.timers),
    });
    ctx.set_host(host.clone());
    core.host = Some(host);

    let mut reply: Option<Rc<Envelope>> = None;
    let mut control: Option<Control> = None;
    let hook = callback(&ctx);
    let mut effects = ctx.effects();
    if let Ok(extra) = &hook {
        effects.extend(extra.clone());
    }
    let split = effects
        .iter()
        .position(|effect| matches!(effect, Effect::Barrier { .. } | Effect::Timer { .. }));
    let pre: Vec<Effect> = split
        .map(|index| effects[..=index].to_vec())
        .unwrap_or_else(|| effects.clone());
    let post: Vec<Effect> = split
        .map(|index| effects[index + 1..].to_vec())
        .unwrap_or_default();
    let applied = apply_effects(&ctx, &pre);
    if close.get() {
        core.begin_close();
    }
    let hook = match (hook, applied) {
        (Ok(_), Ok(())) => Ok(()),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    };
    let wait_spec = pre.iter().find_map(|effect| match effect {
        Effect::Barrier { up_to } => Some((
            true,
            up_to.unwrap_or_else(|| core.records.borrow().next_seq().saturating_sub(1)),
        )),
        Effect::Timer { deadline_ms } => Some((false, *deadline_ms)),
        _ => None,
    });
    let commit = seal_context(&ctx);
    let hold = commit.hold;
    let outcome = match hook {
        Ok(()) => (|| -> Result<(), BoxError> {
            let (sent_reply, sent_control) =
                commit_to(core, bus, clock, id_gen, job, commit, wait_spec.is_some())?;
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

    let delivered = deliver_ending(core, bus, job, reply, control);
    if delivered.is_ok() {
        if let Some((barrier, at)) = wait_spec {
            let bus2 = bus.clone();
            let ids = Rc::clone(id_gen);
            let job2 = job.clone();
            let resume: crate::job::core::ResumeHook = Box::new(move |core: &mut Core| {
                let Some(clock) = core.clock.clone() else {
                    return;
                };
                if post.is_empty() {
                    let hooks = Rc::clone(&job2.hooks);
                    let _ = run_hook(
                        core,
                        &bus2,
                        &*clock,
                        &ids,
                        &job2,
                        Box::new(move |ctx| hooks.on_resume(ctx)),
                    );
                } else {
                    let next = post;
                    let _ = run_hook(
                        core,
                        &bus2,
                        &*clock,
                        &ids,
                        &job2,
                        Box::new(move |_ctx| Ok(next)),
                    );
                }
            });
            if barrier {
                core.defer_barrier(at, resume);
            } else {
                core.defer_timer(at, resume);
            }
        }
    }
    delivered
}
