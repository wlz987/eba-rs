use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;

use eba_rs::envelope::idgen::{IdGen, SeqIdGen};
use eba_rs::envelope::ids::actor_id;
use eba_rs::envelope::result::ResultBody;
use eba_rs::errors::BoxError;
use eba_rs::job::call::{settled, Call};
use eba_rs::job::caller::{CallOptions, Caller};
use eba_rs::job::job::{Context, JobHooks, SharedIdGen};
use eba_rs::job::Service;
use eba_rs::mail::{Bus, Inbox};
use eba_rs::runtime::Runtime;
use eba_rs::time::ManualClock;
use eba_rs::Payload;

struct Hold {
    seen: Rc<RefCell<Option<String>>>,
    begun: Rc<Cell<usize>>,
}

impl JobHooks for Hold {
    fn begin(&self, ctx: &Context) -> Result<(), BoxError> {
        self.begun.set(self.begun.get() + 1);
        ctx.hold()?;
        Ok(())
    }

    fn on_finished(&self, body: Option<&ResultBody>) -> Result<(), BoxError> {
        if let Some(ResultBody::Err { error, .. }) = body {
            *self.seen.borrow_mut() = Some(error.clone());
        }
        Ok(())
    }
}

struct Active {
    seen: Rc<RefCell<Option<String>>>,
}

impl JobHooks for Active {
    fn begin(&self, ctx: &Context) -> Result<(), BoxError> {
        ctx.request("inner", "orphan.work", Arc::new(3u64) as Payload, None)?;
        Ok(())
    }

    fn on_finished(&self, body: Option<&ResultBody>) -> Result<(), BoxError> {
        if let Some(ResultBody::Err { error, .. }) = body {
            *self.seen.borrow_mut() = Some(error.clone());
        }
        Ok(())
    }
}

struct Bad;

impl JobHooks for Bad {
    fn begin(&self, ctx: &Context) -> Result<(), BoxError> {
        ctx.request("inner", "orphan.work", Arc::new(4u64) as Payload, None)?;
        Ok(())
    }

    fn on_finished(&self, _body: Option<&ResultBody>) -> Result<(), BoxError> {
        panic!("cleanup boom")
    }
}

fn main() -> Result<(), BoxError> {
    let ids: SharedIdGen = Rc::new(RefCell::new(Box::new(SeqIdGen::new(1)) as Box<dyn IdGen>));
    let mut runtime = Runtime::new(Bus::new(), Rc::new(ManualClock::new(0)), ids.clone());
    let seen: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));
    let begun: Rc<Cell<usize>> = Rc::new(Cell::new(0));
    let sink = Rc::clone(&seen);
    let counter = Rc::clone(&begun);
    runtime.add(Service::mount("hold", 8, &["orphan.work"], move |_root| {
        Hold {
            seen: Rc::clone(&sink),
            begun: Rc::clone(&counter),
        }
    })?)?;
    let active_seen: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));
    let active_sink = Rc::clone(&active_seen);
    runtime.add(Service::mount("active", 8, &["active"], move |_root| {
        Active {
            seen: Rc::clone(&active_sink),
        }
    })?)?;
    runtime.add(Service::mount("bad", 8, &["bad"], |_root| Bad)?)?;

    let caller = Caller::new(Inbox::new(8).receiver(), ids, 256);
    runtime.add(caller.clone())?;
    let envelope = caller.admit(CallOptions {
        topic: "orphan.work".into(),
        payload: Arc::new(1u64) as Payload,
        from: actor_id("orphan"),
        timeout_ms: None,
    })?;
    for _ in 0..12 {
        runtime.advance()?;
    }
    let cancelled = caller.cancel(&envelope.header.id)?;
    for _ in 0..12 {
        runtime.advance()?;
    }

    let caller_ok = settled(&envelope)
        .as_ref()
        .and_then(|completion| completion.body.as_ref())
        .is_some_and(|body| matches!(body, ResultBody::Err { error, .. } if error == "cancelled"));
    let holder_ok = seen.borrow().as_deref() == Some("cancelled");
    if !cancelled || !caller_ok || !holder_ok {
        return Err(format!(
            "orphan caller={cancelled} caller_ok={caller_ok} holder={:?}",
            seen.borrow()
        )
        .into());
    }
    let before = begun.get();
    let queued = caller.admit(CallOptions {
        topic: "orphan.work".into(),
        payload: Arc::new(2u64) as Payload,
        from: actor_id("orphan"),
        timeout_ms: None,
    })?;
    runtime.advance()?;
    let queued_cancelled = caller.cancel(&queued.header.id)?;
    for _ in 0..4 {
        runtime.advance()?;
    }
    let queued_ok = settled(&queued)
        .as_ref()
        .and_then(|completion| completion.body.as_ref())
        .is_some_and(|body| matches!(body, ResultBody::Err { error, .. } if error == "cancelled"));
    if !queued_cancelled || begun.get() != before || !queued_ok {
        return Err(format!(
            "orphan queued begun={} before={before} cancelled={queued_cancelled} body={:?}",
            begun.get(),
            settled(&queued).and_then(|completion| completion.body)
        )
        .into());
    }
    let active_root = caller.admit(CallOptions {
        topic: "active".into(),
        payload: Arc::new(0u64) as Payload,
        from: actor_id("orphan"),
        timeout_ms: None,
    })?;
    for _ in 0..8 {
        runtime.advance()?;
    }
    let active_cancelled = caller.cancel(&active_root.header.id)?;
    for _ in 0..8 {
        runtime.advance()?;
    }
    let active_ok = settled(&active_root)
        .as_ref()
        .and_then(|completion| completion.body.as_ref())
        .is_some_and(|body| matches!(body, ResultBody::Err { error, .. } if error == "cancelled"));
    let active_holder = active_seen.borrow().as_deref() == Some("cancelled");
    if !active_cancelled || !active_ok || !active_holder || begun.get() != 2 {
        return Err(format!(
            "orphan active cancelled={active_cancelled} ok={active_ok} holder={:?} begun={}",
            active_seen.borrow(),
            begun.get()
        )
        .into());
    }
    let bad_root = caller.admit(CallOptions {
        topic: "bad".into(),
        payload: Arc::new(0u64) as Payload,
        from: actor_id("orphan"),
        timeout_ms: None,
    })?;
    for _ in 0..8 {
        runtime.advance()?;
    }
    let bad_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let bad_cancelled = caller.cancel(&bad_root.header.id)?;
    for _ in 0..8 {
        runtime.advance()?;
    }
    std::panic::set_hook(bad_hook);
    let stats = runtime.stats();
    if !bad_cancelled
        || stats.cancel_errors != 1
        || stats.last_cancel_error.as_deref() != Some("cleanup panic")
    {
        return Err(format!(
            "orphan bad hook cancelled={bad_cancelled} errors={} last={:?} active={}",
            stats.cancel_errors, stats.last_cancel_error, stats.active
        )
        .into());
    }
    let pay_root = caller.admit(CallOptions {
        topic: "orphan.work".into(),
        payload: Arc::new(5u64) as Payload,
        from: actor_id("orphan"),
        timeout_ms: None,
    })?;
    for _ in 0..4 {
        runtime.advance()?;
    }
    if let Some(call) = pay_root.payload.downcast_ref::<Call>() {
        call.set_pay(Box::new(|_| panic!("pay boom")));
    }
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let pay_cancelled = caller.cancel(&pay_root.header.id)?;
    for _ in 0..4 {
        runtime.advance()?;
    }
    std::panic::set_hook(previous_hook);
    if !pay_cancelled || runtime.stats().active != 0 {
        return Err("orphan pay panic leaked".into());
    }
    println!("orphan ok");
    Ok(())
}
