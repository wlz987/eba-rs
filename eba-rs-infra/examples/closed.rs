use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;

use eba_rs_infra::envelope::idgen::{IdGen, SeqIdGen};
use eba_rs_infra::envelope::ids::actor_id;
use eba_rs_infra::errors::BoxError;
use eba_rs_infra::job::caller::{CallOptions, Caller};
use eba_rs_infra::job::job::{Context, JobHooks, SharedIdGen};
use eba_rs_infra::job::{Reply, Service};
use eba_rs_infra::mail::{Bus, Inbox};
use eba_rs_infra::runtime::Runtime;
use eba_rs_infra::time::ManualClock;
use eba_rs_infra::Payload;

fn shared_ids() -> SharedIdGen {
    Rc::new(RefCell::new(Box::new(SeqIdGen::new(1)) as Box<dyn IdGen>))
}

struct Hold {
    cancelled: Rc<Cell<bool>>,
    reply: Rc<RefCell<Option<Reply>>>,
}

impl JobHooks for Hold {
    fn begin(&self, ctx: &Context) -> Result<(), BoxError> {
        let reply = ctx.hold()?;
        let flag = Rc::clone(&self.cancelled);
        reply.on_cancel(move || flag.set(true));
        *self.reply.borrow_mut() = Some(reply);
        Ok(())
    }
}

fn main() -> Result<(), BoxError> {
    let ids = shared_ids();
    let mut runtime = Runtime::new(Bus::new(), Rc::new(ManualClock::new(0)), ids.clone());
    let cancelled = Rc::new(Cell::new(false));
    let held: Rc<RefCell<Option<Reply>>> = Rc::new(RefCell::new(None));
    let flag = Rc::clone(&cancelled);
    let slot = Rc::clone(&held);
    runtime.add(Service::mount("hold", 4, &["closed.hold"], move |_root| {
        Hold {
            cancelled: Rc::clone(&flag),
            reply: Rc::clone(&slot),
        }
    })?)?;
    let caller = Caller::new(Inbox::new(4).receiver(), ids, 256);
    runtime.add(caller.clone())?;
    caller.admit(CallOptions {
        topic: "closed.hold".into(),
        payload: Arc::new(1_u64) as Payload,
        from: actor_id("closed"),
        timeout_ms: None,
    })?;
    for _ in 0..10 {
        if held.borrow().is_some() {
            break;
        }
        runtime.advance()?;
    }
    if held.borrow().is_none() {
        return Err("job never held".into());
    }
    runtime.close(None)?;
    if !cancelled.get() || !held.borrow().as_ref().is_some_and(Reply::is_cancelled) {
        return Err("held signal not cancelled on close".into());
    }
    match caller.admit(CallOptions {
        topic: "closed.work".into(),
        payload: Arc::new(1_u64) as Payload,
        from: actor_id("closed"),
        timeout_ms: None,
    }) {
        Err(error) => {
            println!("refused: {error}");
            Ok(())
        }
        Ok(_) => Err("admit after close should be refused".into()),
    }
}
