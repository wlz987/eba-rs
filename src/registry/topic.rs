
use crate::envelope::EnvelopeId;
use crate::idgen::IdGenHandle;

pub fn result_topic_of(
    request_id: &EnvelopeId,
    result_prefix: &str,
    gen: &IdGenHandle,
) -> crate::Result<String> {
    let seg = gen.borrow().topic_segment(request_id)?;
    Ok(format!("{result_prefix}.{seg}"))
}
