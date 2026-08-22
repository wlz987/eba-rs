
use crate::bus::Bus;
use crate::envelope::{make_envelope_with, ActorId, Envelope, Value};
use crate::idgen::IdGenHandle;
use crate::registry::topic::result_topic_of;

pub fn reply(
    bus: &Bus,
    request: &Envelope,
    body: &Value,
    result_prefix: &str,
    from: ActorId,
    gen: &IdGenHandle,
) -> crate::Result<()> {
    let topic = result_topic_of(&request.header.id, result_prefix, gen)?;
    let mut payload = Value::object();
    payload.insert("request_id", Value::Str(request.header.id.0.clone()));
    payload.insert("result", body.clone());
    let envelope = make_envelope_with(
        &topic,
        payload,
        from,
        Some(request.header.cause.clone()),
        None,
        gen,
    )?;
    bus.publish(&envelope)
}
