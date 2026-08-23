use std::cell::RefCell;
use std::rc::Rc;

use eba::{make_envelope, ActorId, Envelope, EnvelopeId, IdGen, IdGenHandle, MakeOptions, Value};

pub fn aaaa() -> EnvelopeId {
    EnvelopeId("a".repeat(32))
}

pub fn bbbb() -> EnvelopeId {
    EnvelopeId("b".repeat(32))
}

pub fn cccc() -> EnvelopeId {
    EnvelopeId("c".repeat(32))
}

pub fn object(pairs: Vec<(&str, Value)>) -> Value {
    Value::Object(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

pub fn env_n(topic: &str, n: i64) -> Envelope {
    make_envelope(
        topic,
        Value::Int(n),
        ActorId("a".into()),
        MakeOptions {
            id: Some(EnvelopeId(format!("{n:032x}"))),
            ..Default::default()
        },
    )
    .expect("env")
}

pub struct FixedId {
    pub eid: EnvelopeId,
}

impl IdGen for FixedId {
    fn next_envelope_id(&mut self) -> EnvelopeId {
        self.eid.clone()
    }

    fn topic_segment(&self, id: &EnvelopeId) -> Result<String, eba::Error> {
        eba::topic_segment(id).map_err(eba::Error::from)
    }
}

pub fn fixed(eid: &str) -> IdGenHandle {
    Rc::new(RefCell::new(FixedId {
        eid: EnvelopeId(eid.into()),
    }))
}

pub struct RemapGen;

impl IdGen for RemapGen {
    fn next_envelope_id(&mut self) -> EnvelopeId {
        EnvelopeId("0".into())
    }

    fn topic_segment(&self, id: &EnvelopeId) -> Result<String, eba::Error> {
        let first = id.0.chars().next().unwrap_or('e');
        Ok(format!("z{first}"))
    }
}

pub fn remap() -> IdGenHandle {
    Rc::new(RefCell::new(RemapGen))
}

pub fn seq_gen(start: u64) -> IdGenHandle {
    Rc::new(RefCell::new(eba::SeqIdGen::new(start)))
}

pub fn echo_result(rid: &EnvelopeId, cause: &EnvelopeId, value: i64) -> Envelope {
    let gen = fixed(&rid.0);
    let topic = eba::result_topic_of(rid, "acl.result", &gen).expect("topic");
    let payload = object(vec![
        ("request_id", Value::Str(rid.0.clone())),
        ("result", eba::ok_payload(Value::Int(value))),
    ]);
    make_envelope(
        &topic,
        payload,
        ActorId("l".into()),
        MakeOptions {
            cause: Some(cause.clone()),
            id_gen: Some(seq_gen(1)),
            ..Default::default()
        },
    )
    .expect("echo")
}
