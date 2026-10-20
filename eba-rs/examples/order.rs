use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use eba_rs::drive::Drive;
use eba_rs::envelope::idgen::{IdGen, SeqIdGen};
use eba_rs::envelope::ids::actor_id;
use eba_rs::envelope::result::{
    err_payload, is_result_ok, ok_payload, result_error, result_value, Completion,
};
use eba_rs::envelope::Envelope;
use eba_rs::errors::BoxError;
use eba_rs::job::caller::CallOptions;
use eba_rs::job::caller::Caller;
use eba_rs::job::core::{Core, CoreOptions, MakeJob};
use eba_rs::job::host::JobHost;
use eba_rs::job::job::{Context, Job, JobHooks, SharedIdGen};
use eba_rs::mail::inbox::Receiver;
use eba_rs::mail::{Bus, Inbox};
use eba_rs::runtime::Runtime;
use eba_rs::time::ManualClock;
use eba_rs::Payload;

struct OrderJob;

impl JobHooks for OrderJob {
    fn begin(&self, ctx: &Context) -> Result<(), BoxError> {
        ctx.request(
            "inventory",
            "inventory.reserve",
            ctx.root().payload.clone(),
            None,
        )?;
        Ok(())
    }

    fn on_stage_result(
        &self,
        ctx: &Context,
        stage: &str,
        completion: Rc<Completion>,
    ) -> Result<(), BoxError> {
        let body = completion.body.clone();
        let unanswered = match &body {
            None => true,
            Some(found) => !is_result_ok(found),
        };
        if unanswered {
            let handed = match &body {
                Some(found) => Some(found.clone()),
                None => {
                    let mut detail: HashMap<String, Payload> = HashMap::new();
                    detail.insert("stage".into(), Arc::new(String::from(stage)) as Payload);
                    Some(err_payload("dependency_timeout", Some(detail))?)
                }
            };
            ctx.finish(handed)?;
            return Ok(());
        }
        if stage == "inventory" {
            ctx.request(
                "payment",
                "payment.charge",
                ctx.root().payload.clone(),
                None,
            )?;
            return Ok(());
        }
        let sku = sku_of(&ctx.root().payload);
        ctx.finish(Some(ok_payload(sku_map(&sku))))?;
        Ok(())
    }
}

struct ServiceJob;

impl JobHooks for ServiceJob {
    fn begin(&self, ctx: &Context) -> Result<(), BoxError> {
        let root = ctx.root();
        let topic = root.header.topic.clone();
        let sold_out = sku_of(&root.payload) == "sold-out";
        if topic == "inventory.reserve" && sold_out {
            ctx.finish(Some(err_payload("out_of_stock", None)?))?;
        } else if topic == "inventory.reserve" {
            ctx.finish(Some(ok_payload(
                Arc::new(String::from("reserved")) as Payload
            )))?;
        } else {
            ctx.finish(Some(ok_payload(Arc::new(String::from("paid")) as Payload)))?;
        }
        Ok(())
    }
}

fn sku_of(payload: &Payload) -> String {
    payload
        .downcast_ref::<HashMap<String, Payload>>()
        .and_then(|body| body.get("sku"))
        .and_then(|sku| sku.downcast_ref::<String>())
        .cloned()
        .unwrap_or_default()
}

fn sku_map(sku: &str) -> Payload {
    let mut body: HashMap<String, Payload> = HashMap::new();
    body.insert("sku".into(), Arc::new(String::from(sku)) as Payload);
    Arc::new(body) as Payload
}

struct Host {
    core: Rc<RefCell<Core>>,
    make_job: MakeJob,
}

impl JobHost for Host {
    fn core(&self) -> Rc<RefCell<Core>> {
        self.core.clone()
    }

    fn make_job(&self, root: Rc<Envelope>) -> Result<Job, BoxError> {
        (self.make_job)(root)
    }

    fn on_rejected(&self, _root: Rc<Envelope>, _reason: String) -> Result<(), BoxError> {
        Ok(())
    }
}

fn host(
    actor: &str,
    receiver: Receiver,
    patterns: Vec<String>,
    make_job: MakeJob,
) -> Rc<dyn JobHost> {
    let core = Core::new(CoreOptions {
        actor_id: actor_id(actor),
        receiver,
        patterns,
        request_timeout_ms: None,
        queue_limit: 512,
        active_limit: 256,
        work_budget: 64,
        outbox_limit: 512,
        make_job: make_job.clone(),
        on_rejected: Rc::new(|_, _| Ok(())),
    });
    let core = Rc::new(RefCell::new(core));
    Rc::new(Host {
        core: core.clone(),
        make_job,
    })
}

fn shared_ids() -> SharedIdGen {
    Rc::new(RefCell::new(Box::new(SeqIdGen::new(1)) as Box<dyn IdGen>))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let ids = shared_ids();
            let mut runtime = Runtime::new(Bus::new(), Rc::new(ManualClock::new(0)), ids.clone());
            runtime.add(host(
                "orders",
                Inbox::new(8).receiver(),
                vec!["order.submit".into()],
                Rc::new(|root| Ok(Job::new(root, Rc::new(OrderJob)))),
            ))?;
            runtime.add(host(
                "services",
                Inbox::new(8).receiver(),
                vec!["inventory.reserve".into(), "payment.charge".into()],
                Rc::new(|root| Ok(Job::new(root, Rc::new(ServiceJob)))),
            ))?;
            let caller = Caller::new(Inbox::new(8).receiver(), ids, 256);
            let drive = Drive::new(runtime);
            drive.start();

            let first = drive.open(
                caller.clone(),
                CallOptions {
                    topic: "order.submit".into(),
                    payload: sku_map("A"),
                    from: actor_id("caller.a"),
                    timeout_ms: None,
                },
            )?;
            let second = drive.open(
                caller,
                CallOptions {
                    topic: "order.submit".into(),
                    payload: sku_map("sold-out"),
                    from: actor_id("caller.b"),
                    timeout_ms: None,
                },
            )?;
            println!(
                "收信已返回 {} {}",
                first.envelope.header.id, second.envelope.header.id
            );
            let left = first.ready.await?.expect("甲的定论");
            let right = second.ready.await?.expect("乙的定论");
            drive.close().await?;
            let handed = left.body.as_ref().expect("有正文");
            let won = match result_value(handed) {
                Some(value) => sku_of(value),
                None => String::new(),
            };
            println!("甲 ok sku={won}");
            println!(
                "乙 {}",
                result_error(right.body.as_ref().expect("有正文")).unwrap_or_default()
            );
            Ok(())
        })
        .await
}
