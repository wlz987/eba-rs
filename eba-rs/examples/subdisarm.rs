use eba_rs::errors::BoxError;
use eba_rs::mail::{Bus, Inbox};

fn main() -> Result<(), BoxError> {
    let sub = Bus::new().subscribe("a.b", &Inbox::new(8))?;
    sub.disarm();
    let active = sub.active();
    let closed = sub.close();
    if active || closed {
        return Err("subdisarm leaked".into());
    }
    println!("subdisarm: {active} {closed}");
    Ok(())
}
