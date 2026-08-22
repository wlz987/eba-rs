
use crate::envelope::{topic_suffix, Envelope, EnvelopeId};
use crate::idgen::IdGenHandle;
use crate::registry::State;
use crate::result::body::{result_body, result_request_id};

#[derive(Debug, Clone)]
pub struct Entry {
    pub expected_topic: String,
    pub cause: EnvelopeId,
    pub state: State,
}

pub fn quad_ok(
    request_id: &EnvelopeId,
    entry: &Entry,
    env: &Envelope,
    gen: &IdGenHandle,
) -> bool {
    if result_request_id(env).as_ref() != Some(request_id) {
        return false;
    }
    let seg = match gen.borrow().topic_segment(request_id) {
        Ok(seg) => seg,
        Err(_) => return false,
    };
    let Ok(suffix) = topic_suffix(&env.header.topic) else {
        return false;
    };
    suffix == seg
        && env.header.topic == entry.expected_topic
        && env.header.cause == entry.cause
        && result_body(env).is_some()
}
