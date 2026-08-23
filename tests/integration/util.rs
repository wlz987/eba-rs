use std::cell::RefCell;
use std::rc::Rc;

use eba::{
    err_payload, is_result_err, make_envelope, ok_payload, result_error, result_value, ActorId,
    Envelope, EnvelopeId, FinishInfo, HostParams, IdGenHandle, InboxHandle, Job, JobHost,
    RequestSpec, Value,
};

pub fn actor_a() -> ActorId {
    ActorId("a".into())
}

pub fn actor_h() -> ActorId {
    ActorId("host".into())
}

pub fn actor_l() -> ActorId {
    ActorId("leaf".into())
}

pub fn env_gen(topic: &str, payload: Value, gen: &IdGenHandle) -> Envelope {
    make_envelope(
        topic,
        payload,
        actor_a(),
        eba::MakeOptions {
            id_gen: Some(gen.clone()),
            ..Default::default()
        },
    )
    .expect("env")
}

pub fn seq(start: u64) -> IdGenHandle {
    Rc::new(RefCell::new(eba::SeqIdGen::new(start)))
}

pub fn read_job(root: Envelope) -> Job {
    let mut j = Job::new(root);
    j.on_begin = Some(Box::new(|ctx| {
        let payload = ctx.job.root.payload.clone();
        ctx.reply("read.result", &ok_payload(payload.clone()))?;
        ctx.finish(FinishInfo::ok_answer(payload))
    }));
    j
}

pub fn echo_job(root: Envelope) -> Job {
    let mut j = Job::new(root);
    j.on_begin = Some(Box::new(|ctx| {
        ctx.reply("acl.result", &ok_payload(Value::Str("allow".into())))?;
        ctx.finish(FinishInfo::ok_answer(Value::Str("allow".into())))
    }));
    j
}

pub fn wait_job(root: Envelope, begins: Rc<std::cell::Cell<u32>>) -> Job {
    let mut j = Job::new(root);
    j.on_begin = Some(Box::new(move |ctx| {
        begins.set(begins.get() + 1);
        ctx.request(RequestSpec {
            stage: "ext".into(),
            request_prefix: "ext.wait".into(),
            result_prefix: "ext.wait.result".into(),
            payload: Value::Null,
        })?;
        Ok(())
    }));
    j.on_stage_result = Some(Box::new(|ctx, _stage, body| {
        if is_result_err(body) {
            let msg = result_error(body).unwrap_or("err").to_string();
            ctx.reply("wait.result", body)?;
            return ctx.finish(FinishInfo::err_msg(msg));
        }
        let v = result_value(body).cloned().unwrap_or(Value::Null);
        ctx.reply("wait.result", &ok_payload(v.clone()))?;
        ctx.finish(FinishInfo::ok_answer(v))
    }));
    j
}

#[derive(Default, Clone)]
pub struct StepState {
    pub seen: Vec<String>,
    pub child_id: Option<EnvelopeId>,
}

pub type States = Rc<RefCell<Vec<StepState>>>;

pub fn new_states() -> States {
    Rc::new(RefCell::new(vec![]))
}

pub fn step_job(root: Envelope, states: &States) -> Job {
    let mut j = Job::new(root);
    let st = states.clone();
    j.on_begin = Some(Box::new(move |ctx| {
        let id = ctx.request(RequestSpec {
            stage: "acl".into(),
            request_prefix: "acl".into(),
            result_prefix: "acl.result".into(),
            payload: crate::util::object(vec![("who", Value::Str("x".into()))]),
        })?;
        st.borrow_mut().push(StepState {
            seen: vec![],
            child_id: Some(id),
        });
        Ok(())
    }));
    let st2 = states.clone();
    j.on_stage_result = Some(Box::new(move |ctx, stage, body| {
        st2.borrow_mut()
            .last_mut()
            .expect("state recorded at begin")
            .seen
            .push(stage.to_string());
        if is_result_err(body) {
            let msg = result_error(body).unwrap_or("err").to_string();
            return ctx.finish(FinishInfo {
                ok: false,
                error: Some(msg),
                answer: None,
            });
        }
        let v = result_value(body).cloned().unwrap_or(Value::Null);
        ctx.reply("job.result", &ok_payload(v.clone()))?;
        ctx.finish(FinishInfo::ok_answer(v))
    }));
    j
}

pub fn step_host(box_: InboxHandle, timeout_ms: i64, queue_limit: usize) -> (JobHost, States) {
    let states: States = Rc::new(RefCell::new(vec![]));
    let st = states.clone();
    let host = JobHost::new(HostParams {
        actor_id: actor_h(),
        inbox: box_,
        patterns: vec!["job".to_string()],
        accept: None,
        request_timeout_ms: timeout_ms,
        queue_limit,
        make_job: Box::new(move |root| step_job(root.clone(), &st)),
    });
    (host, states)
}

pub fn wait_host(timeout_ms: i64) -> (JobHost, Rc<std::cell::Cell<u32>>) {
    let begins = Rc::new(std::cell::Cell::new(0));
    let b = begins.clone();
    let host = JobHost::new(HostParams {
        actor_id: actor_h(),
        inbox: eba::Inbox::new(8),
        patterns: vec!["wait".to_string()],
        accept: None,
        request_timeout_ms: timeout_ms,
        queue_limit: 0,
        make_job: Box::new(move |root| wait_job(root.clone(), b.clone())),
    });
    (host, begins)
}

pub fn leaf_host() -> JobHost {
    JobHost::new(HostParams {
        actor_id: actor_l(),
        inbox: eba::Inbox::new(8),
        patterns: vec!["acl.*".to_string()],
        accept: None,
        request_timeout_ms: 0,
        queue_limit: 0,
        make_job: Box::new(|root| echo_job(root.clone())),
    })
}

pub fn combo_host(states: States) -> JobHost {
    let st = states.clone();
    JobHost::new(HostParams {
        actor_id: actor_h(),
        inbox: eba::Inbox::new(8),
        patterns: vec!["job".to_string(), "acl.*".to_string()],
        accept: None,
        request_timeout_ms: 0,
        queue_limit: 0,
        make_job: Box::new(move |root| {
            if root.header.topic.starts_with("acl") {
                echo_job(root.clone())
            } else {
                step_job(root.clone(), &st)
            }
        }),
    })
}

pub fn object(pairs: Vec<(&str, Value)>) -> Value {
    Value::Object(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

pub fn result_echo(
    rid: &EnvelopeId,
    cause: &EnvelopeId,
    body: Value,
    gen: &IdGenHandle,
) -> Envelope {
    let topic = eba::result_topic_of(rid, "acl.result", gen).unwrap();
    make_envelope(
        &topic,
        object(vec![
            ("request_id", Value::Str(rid.0.clone())),
            ("result", body),
        ]),
        actor_l(),
        eba::MakeOptions {
            cause: Some(cause.clone()),
            id_gen: Some(gen.clone()),
            ..Default::default()
        },
    )
    .expect("echo")
}

pub fn err_body(msg: &str) -> Value {
    err_payload(msg, &[]).unwrap()
}
