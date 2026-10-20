use eba_rs_infra::errors::BoxError;
use eba_rs_infra::mail::{Bus, Inbox};

fn main() -> Result<(), BoxError> {
    let bus = Bus::new();
    let inbox = Inbox::new(8);
    let first = bus.subscribe("a.b", &inbox)?;
    let second = bus.subscribe("a.b", &inbox)?;
    let closed_first = first.close();
    let closed_second = second.close();
    if !closed_first || !closed_second || first.active() || second.active() {
        return Err("subdup leaked".into());
    }
    println!(
        "subdup: {closed_first} {closed_second} {} {}",
        first.active(),
        second.active()
    );
    Ok(())
}
