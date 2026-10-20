use std::rc::Rc;

use eba_rs::envelope::ids::actor_id;
use eba_rs::errors::BoxError;
use eba_rs::job::core::{Core, CoreOptions};
use eba_rs::job::job::{Job, JobHooks};
use eba_rs::mail::Inbox;

struct SilentHooks;
impl JobHooks for SilentHooks {}

fn main() -> Result<(), BoxError> {
    let options = CoreOptions {
        actor_id: actor_id("x"),
        receiver: Inbox::new(8).receiver(),
        patterns: vec![],
        request_timeout_ms: None,
        queue_limit: 512,
        active_limit: 0,
        work_budget: 64,
        outbox_limit: 512,
        make_job: Rc::new(|root| Ok(Job::new(root, Rc::new(SilentHooks)))),
        on_rejected: Rc::new(|_, _| Ok(())),
    };
    match Core::try_new(options) {
        Err(error) => {
            println!("refused: {error}");
            Ok(())
        }
        Ok(_) => Err("zero active limit should be refused".into()),
    }
}
