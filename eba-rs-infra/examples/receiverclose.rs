use eba_rs_infra::errors::BoxError;
use eba_rs_infra::mail::Inbox;

fn main() -> Result<(), BoxError> {
    let inbox = Inbox::new(8);
    let receiver = inbox.receiver();
    let first = receiver.close();
    let second = receiver.close();
    if !first || second || !inbox.is_closed() {
        return Err("receiverclose leaked".into());
    }
    println!("receiverclose: {first} {second} {}", inbox.is_closed());
    Ok(())
}
