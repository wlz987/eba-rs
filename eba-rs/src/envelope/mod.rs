#![allow(clippy::module_inception)]

pub mod envelope;
pub mod header;
pub mod idgen;
pub mod ids;
pub mod reply;
pub mod result;
pub mod topic;

pub use envelope::{make_envelope, Envelope, MakeEnvelope};
pub use header::{Address, Header};
pub use ids::{actor_id, envelope_id, ActorId, EnvelopeId};
pub use reply::Matchmaker;
pub use result::{
    err_payload, err_payload_code, ok_payload, result_body, result_code, result_error,
    result_request_id, result_value, Completion, ResultBody,
};
