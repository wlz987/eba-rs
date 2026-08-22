
pub(crate) mod error;
pub(crate) mod id;
pub(crate) mod topic;
pub(crate) mod value;

pub use error::EnvelopeBuildError;
pub use id::{ActorId, EnvelopeId};
pub use topic::{is_name, split_topic, topic_suffix};
pub use value::Value;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Header {
    pub id: EnvelopeId,
    pub topic: String,
    pub from: ActorId,
    pub cause: EnvelopeId,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Envelope {
    pub header: Header,
    pub payload: Value,
}

#[derive(Debug, Default, Clone)]
pub struct MakeOptions {
    pub cause: Option<EnvelopeId>,
    pub id: Option<EnvelopeId>,
    pub id_gen: Option<crate::idgen::IdGenHandle>,
}

pub fn make_envelope(
    topic: &str,
    payload: impl Into<Value>,
    from: ActorId,
    opts: MakeOptions,
) -> Result<Envelope, EnvelopeBuildError> {
    split_topic(topic)?;
    let eid = match opts.id {
        Some(id) => id,
        None => match opts.id_gen {
            Some(gen) => gen.borrow_mut().next_envelope_id(),
            None => {
                return Err(EnvelopeBuildError::new(
                    "make_envelope requires id or id_gen",
                ))
            }
        },
    };
    let cause = opts.cause.unwrap_or_else(|| eid.clone());
    Ok(Envelope {
        header: Header {
            id: eid,
            topic: topic.into(),
            from,
            cause,
        },
        payload: payload.into(),
    })
}

pub(crate) fn make_envelope_with(
    topic: &str,
    payload: Value,
    from: ActorId,
    cause: Option<EnvelopeId>,
    id: Option<EnvelopeId>,
    gen: &crate::idgen::IdGenHandle,
) -> Result<Envelope, EnvelopeBuildError> {
    make_envelope(
        topic,
        payload,
        from,
        MakeOptions {
            cause,
            id,
            id_gen: Some(gen.clone()),
        },
    )
}

pub fn topic_segment(id: &EnvelopeId) -> Result<String, EnvelopeBuildError> {
    crate::envelope::id::topic_segment(id)
}
