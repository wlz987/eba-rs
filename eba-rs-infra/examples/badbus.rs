use eba_rs_infra::errors::BoxError;
use eba_rs_infra::mail::{Bus, Inbox};

fn main() -> Result<(), BoxError> {
    match Bus::new().subscribe("bad!", &Inbox::new(8)) {
        Err(error) => {
            println!("refused: {error}");
            Ok(())
        }
        Ok(_) => Err("bad pattern should be refused by the bus".into()),
    }
}
