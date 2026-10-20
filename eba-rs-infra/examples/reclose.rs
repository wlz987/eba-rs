use std::cell::RefCell;
use std::rc::Rc;

use eba_rs_infra::envelope::idgen::{IdGen, SeqIdGen};
use eba_rs_infra::errors::BoxError;
use eba_rs_infra::job::job::SharedIdGen;
use eba_rs_infra::mail::Bus;
use eba_rs_infra::runtime::Runtime;
use eba_rs_infra::time::ManualClock;

fn shared_ids() -> SharedIdGen {
    Rc::new(RefCell::new(Box::new(SeqIdGen::new(1)) as Box<dyn IdGen>))
}

fn main() -> Result<(), BoxError> {
    let mut runtime = Runtime::new(Bus::new(), Rc::new(ManualClock::new(0)), shared_ids());
    let first = runtime.close(None)?;
    let second = runtime.close(None)?;
    if !first.is_empty() || !second.is_empty() {
        return Err("expected two empty closes".into());
    }
    println!("reclose: {} {}", first.len(), second.len());
    Ok(())
}
