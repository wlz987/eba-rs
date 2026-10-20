use std::cell::RefCell;
use std::rc::Rc;

use eba_rs_infra::envelope::idgen::{IdGen, SeqIdGen};
use eba_rs_infra::envelope::ids::actor_id;
use eba_rs_infra::envelope::result::ResultBody;
use eba_rs_infra::errors::BoxError;
use eba_rs_infra::job::caller::{settled, CallOptions, Caller};
use eba_rs_infra::job::job::SharedIdGen;
use eba_rs_infra::mail::{Bus, Inbox};
use eba_rs_infra::runtime::Runtime;
use eba_rs_infra::time::ManualClock;
use eba_rs_infra::Payload;

fn shared_ids() -> SharedIdGen {
    Rc::new(RefCell::new(Box::new(SeqIdGen::new(1)) as Box<dyn IdGen>))
}

fn main() -> Result<(), BoxError> {
    let ids = shared_ids();
    let mut runtime = Runtime::new(Bus::new(), Rc::new(ManualClock::new(0)), ids.clone());
    let caller = Caller::new(Inbox::new(8).receiver(), ids, 256);
    runtime.add(caller.clone())?;
    let envelope = caller.admit(CallOptions {
        topic: "cancel.work".into(),
        payload: std::sync::Arc::new(1_u64) as Payload,
        from: actor_id("cancel"),
        timeout_ms: None,
    })?;
    let cancelled = caller.cancel(&envelope.header.id)?;
    for _ in 0..40 {
        if settled(&envelope).is_some() {
            break;
        }
        if !runtime.advance()? {
            break;
        }
    }
    match settled(&envelope).and_then(|completion| completion.body) {
        Some(ResultBody::Err { error, .. }) if error == "cancelled" => {
            println!("cancelled: {cancelled} {error}");
            Ok(())
        }
        other => Err(format!("expected cancelled, got {other:?}").into()),
    }
}
