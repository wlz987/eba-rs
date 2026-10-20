use std::cell::RefCell;
use std::rc::Rc;

use eba_rs::envelope::idgen::{IdGen, SeqIdGen};
use eba_rs::envelope::ids::actor_id;
use eba_rs::errors::BoxError;
use eba_rs::job::caller::{CallOptions, Caller};
use eba_rs::job::job::SharedIdGen;
use eba_rs::mail::{Bus, Inbox};
use eba_rs::runtime::Runtime;
use eba_rs::time::ManualClock;
use eba_rs::Payload;

fn shared_ids() -> SharedIdGen {
    Rc::new(RefCell::new(Box::new(SeqIdGen::new(1)) as Box<dyn IdGen>))
}

fn main() -> Result<(), BoxError> {
    let ids = shared_ids();
    let mut runtime = Runtime::new(Bus::new(), Rc::new(ManualClock::new(0)), ids.clone());
    let caller = Caller::new(Inbox::new(8).receiver(), ids, 1);
    runtime.add(caller.clone())?;
    let _first = caller.admit(CallOptions {
        topic: "bp.work".into(),
        payload: std::sync::Arc::new(1_u64) as Payload,
        from: actor_id("bp"),
        timeout_ms: None,
    })?;
    match caller.admit(CallOptions {
        topic: "bp.work".into(),
        payload: std::sync::Arc::new(2_u64) as Payload,
        from: actor_id("bp"),
        timeout_ms: None,
    }) {
        Err(error) => {
            println!("refused: {error}");
            Ok(())
        }
        Ok(_) => Err("second admit should be backpressured".into()),
    }
}
