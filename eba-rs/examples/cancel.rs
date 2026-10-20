use std::cell::RefCell;
use std::rc::Rc;

use eba_rs::drive::Drive;
use eba_rs::envelope::idgen::{IdGen, SeqIdGen};
use eba_rs::envelope::ids::actor_id;
use eba_rs::envelope::result::ResultBody;
use eba_rs::errors::{BoxError, BusError};
use eba_rs::job::caller::{CallOptions, Caller};
use eba_rs::job::job::SharedIdGen;
use eba_rs::mail::{Bus, Inbox};
use eba_rs::runtime::Runtime;
use eba_rs::time::ManualClock;
use eba_rs::Payload;

fn shared_ids() -> SharedIdGen {
    Rc::new(RefCell::new(Box::new(SeqIdGen::new(1)) as Box<dyn IdGen>))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let ids = shared_ids();
            let runtime = Runtime::new(Bus::new(), Rc::new(ManualClock::new(0)), ids.clone());
            let drive = Drive::new(runtime);
            drive.start();
            let caller = Caller::new(Inbox::new(8).receiver(), ids, 256);
            let pending = drive.open(
                caller.clone(),
                CallOptions {
                    topic: "cancel.work".into(),
                    payload: std::sync::Arc::new(1_u64) as Payload,
                    from: actor_id("cancel"),
                    timeout_ms: None,
                },
            )?;
            let cancelled = caller.cancel(&pending.envelope.header.id)?;
            let completion = pending
                .ready
                .await
                .map_err(|_| Box::new(BusError("pending dropped".into())) as BoxError)??;
            match completion.body {
                Some(ResultBody::Err { error, .. }) if error == "cancelled" => {
                    println!("cancelled: {cancelled} {error}");
                    drive.close().await?;
                    Ok(())
                }
                other => Err(format!("expected cancelled, got {other:?}").into()),
            }
        })
        .await
}
