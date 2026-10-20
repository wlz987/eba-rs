use std::cell::RefCell;
use std::rc::Rc;

use eba_rs::envelope::idgen::{IdGen, SeqIdGen};
use eba_rs::errors::BoxError;
use eba_rs::job::job::SharedIdGen;
use eba_rs::mail::Bus;
use eba_rs::runtime::Runtime;
use eba_rs::time::ManualClock;

fn shared_ids() -> SharedIdGen {
    Rc::new(RefCell::new(Box::new(SeqIdGen::new(1)) as Box<dyn IdGen>))
}

fn main() -> Result<(), BoxError> {
    let mut runtime = Runtime::new(Bus::new(), Rc::new(ManualClock::new(0)), shared_ids());
    runtime.close(None)?;
    match runtime.advance() {
        Err(error) => {
            println!("refused: {error}");
            Ok(())
        }
        Ok(_) => Err("advance after close should be refused".into()),
    }
}
