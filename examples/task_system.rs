use std::rc::Rc;

use eba::{
    make_envelope, result_value, ActorId, FinishInfo, HostParams, Inbox, Job, JobHost, Value,
};

fn main() {
    let bus = eba::Bus::new();
    let clk = Rc::new(eba::MonotonicClock::new());
    let gen = Rc::new(std::cell::RefCell::new(eba::SeqIdGen::new(1)));

    let mut reader = JobHost::new(HostParams {
        actor_id: ActorId("reader".into()),
        inbox: Inbox::new(8),
        patterns: vec!["read".into()],
        accept: None,
        request_timeout_ms: 0,
        queue_limit: 0,
        make_job: Box::new(|root| {
            let mut j = Job::new(root.clone());
            j.on_begin = Some(Box::new(|ctx| {
                let payload = ctx.job.root.payload.clone();
                ctx.reply("read.result", &eba::ok_payload(payload.clone()))?;
                ctx.finish(FinishInfo::ok_answer(payload))
            }));
            j
        }),
    });
    reader.start(&bus).unwrap();

    let answers = Inbox::new(8);
    bus.subscribe("read.result.**", &answers).unwrap();

    let req = make_envelope(
        "read",
        Value::Str("buf.a".into()),
        ActorId("client".into()),
        eba::MakeOptions {
            id_gen: Some(gen.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    bus.publish(&req).unwrap();
    loop {
        let more = reader
            .poll(bus.clone(), clk.clone(), gen.clone())
            .unwrap();
        if !more {
            break;
        }
    }

    let got = answers.try_recv().expect("answer");
    let body = got.payload.get("result").cloned().expect("result");
    println!("answer: {:?}", result_value(&body));
}
