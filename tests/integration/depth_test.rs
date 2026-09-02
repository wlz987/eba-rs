//! 深度 3 请求链:根 Job(eng)→ 子请求 mid(其根为子请求信封,id≠cause)→ mid 的子请求 leaf;
//! leaf 的结果须精确认亲到 mid,mid 的应答再认亲到 eng(叶子槽位按子请求 id)。

use crate::util::{env_gen, seq};
use eba::{ActorId, Bus, FinishInfo, HostParams, Inbox, Job, JobHost, RequestSpec, Value};
use std::cell::RefCell;
use std::rc::Rc;

fn chain_host(tag: &str, seen: Rc<RefCell<Vec<String>>>, next: Option<(&str, &str)>) -> JobHost {
    let tag = tag.to_string();
    let next = next.map(|(d, r)| (d.to_string(), r.to_string()));
    JobHost::new(HostParams {
        actor_id: ActorId(tag.clone()),
        inbox: Inbox::new(16),
        patterns: vec![tag.clone(), format!("{tag}.*")],
        accept: None,
        request_timeout_ms: 0,
        queue_limit: 16,
        make_job: Box::new(move |root| {
            let seen = seen.clone();
            let next = next.clone();
            let mut j = Job::new(root.clone());
            let begin_tag = tag.clone();
            j.on_begin = Some(Box::new(move |ctx| {
                seen.borrow_mut().push(begin_tag.clone());
                match next.as_ref() {
                    Some((dst, rp)) => ctx
                        .request(RequestSpec {
                            stage: "d3".into(),
                            request_prefix: dst.clone(),
                            result_prefix: rp.clone(),
                            payload: Value::Null,
                        })
                        .map(|_| ()),
                    None => {
                        let body = eba::ok_payload(Value::Str("leaf".into()));
                        ctx.reply("leaf.result", &body)?;
                        ctx.finish(FinishInfo::ok_answer(Value::Null))
                    }
                }
            }));
            let stage_tag = tag.clone();
            j.on_stage_result = Some(Box::new(move |ctx, _stage, _body| {
                let rp = if stage_tag == "eng" { "eng.result" } else { "mid.result" };
                let body = eba::ok_payload(Value::Str("done".into()));
                ctx.reply(rp, &body)?;
                ctx.finish(FinishInfo::ok_answer(Value::Null))
            }));
            j
        }),
    })
}

#[test]
fn depth_three_chain_delivers() {
    let bus = Bus::new();
    let clk = Rc::new(eba::MonotonicClock::new());
    let gen = seq(1);
    let seen = Rc::new(RefCell::new(Vec::new()));
    let leaf = chain_host("leaf", seen.clone(), None);
    let mid = chain_host("mid", seen.clone(), Some(("leaf", "leaf.result")));
    let eng = chain_host("eng", seen.clone(), Some(("mid", "mid.result")));
    let mut hosts: Vec<RefCell<JobHost>> =
        vec![RefCell::new(leaf), RefCell::new(mid), RefCell::new(eng)];
    for h in &hosts {
        h.borrow().start(&bus).unwrap();
    }
    let dest = Inbox::new(8);
    bus.subscribe("eng.result.**", &dest).unwrap();
    let root = env_gen("eng", Value::Null, &gen);
    hosts[2]
        .borrow_mut()
        .handle(bus.clone(), &root, clk.clone(), gen.clone())
        .unwrap();
    let mut got = None;
    for _ in 0..1000 {
        let mut progress = false;
        for h in &mut hosts {
            progress |= h.borrow_mut().poll(bus.clone(), clk.clone(), gen.clone()).unwrap();
        }
        if let Some(e) = dest.try_recv() {
            got = Some(e);
            break;
        }
        if !progress {
            break;
        }
    }
    let e = got.expect("depth-3 chain must reply to root");
    assert!(eba::is_result_ok(e.payload.get("result").unwrap()));
    assert_eq!(
        &*seen.borrow(),
        &vec!["eng".to_string(), "mid".to_string(), "leaf".to_string()]
    );
}
