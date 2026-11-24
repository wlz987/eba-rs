//! ask → 反复 advance → pending.ready 兑现。
use eba_rs::*;
use std::rc::Rc;
struct HelloJob;
impl JobHooks for HelloJob {
    fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
        ctx.finish(ok_payload(Value::Map({
            let mut m = std::collections::BTreeMap::new();
            m.insert("hi".into(), Value::Bool(true));
            m
        })));
        Vec::new()
    }
}
fn main() {
    let id_gen: Rc<dyn IdGen> = Rc::new(SeqIdGen::new());
    let clock: Rc<dyn Clock> = Rc::new(ManualClock::new(0));
    let mut runtime = Runtime::new(Bus::new(), clock, id_gen).unwrap();
    let host = JobHost {
        actor_id: "hello".into(),
        inbox: Inbox::new(64),
        patterns: vec![parse_pattern("greet").unwrap()],
        make_job: Box::new(|_| Box::new(HelloJob)),
    };
    runtime.add(host).unwrap();
    let pending = runtime.ask("greet", Value::Null, "caller", None);
    for _ in 0..20 {
        runtime.advance().unwrap();
        if pending.is_ready() {
            break;
        }
    }
    let completion = pending.ready().expect("pending should settle");
    println!("completion: {:?}", completion.body);
}
