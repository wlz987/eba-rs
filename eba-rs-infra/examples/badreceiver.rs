use eba_rs_infra::errors::BoxError;
use eba_rs_infra::mail::Inbox;

fn main() -> Result<(), BoxError> {
    let inbox = Inbox::new(8);
    let _first = inbox.receiver();
    match inbox.try_receiver() {
        Err(error) => {
            println!("refused: {error}");
            Ok(())
        }
        Ok(_) => Err("second receiver should be refused".into()),
    }
}
