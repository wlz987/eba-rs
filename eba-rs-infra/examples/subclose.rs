use eba_rs_infra::errors::BoxError;
use eba_rs_infra::mail::{Bus, Inbox};

fn main() -> Result<(), BoxError> {
    let sub = Bus::new().subscribe("a.b", &Inbox::new(8))?;
    let first = sub.close();
    let second = sub.close();
    if !first || second {
        return Err("subclose leaked".into());
    }
    println!("subclose: {first} {second}");
    Ok(())
}
