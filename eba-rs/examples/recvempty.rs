use eba_rs::errors::BoxError;
use eba_rs::mail::Inbox;

fn main() -> Result<(), BoxError> {
    let receiver = Inbox::new(8).receiver();
    let received = receiver.try_recv()?;
    if received.is_some() {
        return Err("recvempty leaked".into());
    }
    println!("recvempty: none");
    Ok(())
}
