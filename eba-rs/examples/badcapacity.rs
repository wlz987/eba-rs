use eba_rs::errors::BoxError;
use eba_rs::mail::Inbox;

fn main() -> Result<(), BoxError> {
    match Inbox::try_new(0) {
        Err(error) => {
            println!("refused: {error}");
            Ok(())
        }
        Ok(_) => Err("zero capacity should be refused".into()),
    }
}
