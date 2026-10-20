use eba_rs_infra::errors::BoxError;
use eba_rs_infra::mail::Inbox;

fn main() -> Result<(), BoxError> {
    let receiver = Inbox::new(8).receiver();
    let received = receiver.try_recv()?;
    if received.is_some() {
        return Err("recvempty leaked".into());
    }
    println!("recvempty: none");
    Ok(())
}
