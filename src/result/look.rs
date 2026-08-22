
use crate::envelope::{topic_suffix, Envelope};
use crate::idgen::IdGenHandle;
use crate::result::body::{result_body, result_request_id};

pub fn looks_like_result_envelope(env: &Envelope, gen: &IdGenHandle) -> bool {
    let Some(request_id) = result_request_id(env) else {
        return false;
    };
    if result_body(env).is_none() {
        return false;
    }
    let seg = match gen.borrow().topic_segment(&request_id) {
        Ok(seg) => seg,
        Err(_) => return false,
    };
    matches!(topic_suffix(&env.header.topic), Ok(suffix) if suffix == seg)
}
