
pub mod bus;
pub mod clock;
pub mod envelope;
pub mod idgen;
pub mod inbox;
pub mod job;
mod jobhost;
pub mod pattern;
pub mod publisher;
pub mod registry;
pub mod reply;
pub mod result;
pub mod subscriber;

pub use bus::{Bus, BusHandle};
pub use clock::{Clock, ClockHandle, ManualClock, MonotonicClock};
pub use envelope::{
    make_envelope, topic_segment, topic_suffix, split_topic, is_name,
    ActorId, Envelope, EnvelopeBuildError, EnvelopeId, Header,
    MakeOptions, Value,
};
pub use idgen::{IdGen, IdGenHandle, SeqIdGen, UuidIdGen};
pub use inbox::{Inbox, InboxHandle};
pub use job::{
    timeout_body, FinishInfo, Job, RequestSpec,
    DEFAULT_MAX_INFLIGHT,
};
pub use jobhost::{HostParams, JobHost, DEFAULT_QUEUE_LIMIT};
pub use pattern::{matches, parse_pattern, InvalidTopic, Pattern, Wildcard};
pub use publisher::Publisher;
pub use registry::{result_topic_of, Registry, ResolveOutcome, StartParams, State};
pub use reply::reply;
pub use result::{
    err_payload, is_result_err, is_result_ok,
    looks_like_result_envelope, ok_payload, result_body, result_error,
    result_request_id, result_value,
};
pub use subscriber::Subscriber;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    EnvelopeBuild(String),
    InvalidTopic(String),
    MailboxFull,
    QueueFull,
    MaxInflight,
    State(String),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Error::EnvelopeBuild(m) => write!(f, "envelope: {m}"),
            Error::InvalidTopic(m) => write!(f, "pattern: {m}"),
            Error::MailboxFull => write!(f, "bus: mailbox full"),
            Error::QueueFull => write!(f, "jobhost: queue_full"),
            Error::MaxInflight => write!(f, "job: max_inflight"),
            Error::State(m) => write!(f, "job: {m}"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = core::result::Result<T, Error>;
