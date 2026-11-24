#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]
extern crate alloc;
/// 统一容纳 std / no_std 的类型重导出。
pub(crate) mod prelude {
    pub use alloc::boxed::Box;
    pub use alloc::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
    pub use alloc::format;
    pub use alloc::rc::Rc;
    pub use alloc::string::{String, ToString};
    pub use alloc::vec;
    pub use alloc::vec::Vec;
    pub use core::cell::{Cell, RefCell};
}
pub mod envelope;
pub mod errors;
pub mod job;
pub mod mail;
pub mod notify;
pub mod pattern;
pub mod pool;
pub mod record;
pub mod runtime;
pub mod schema;
pub mod time;
pub mod value;
// --- 公共 API 汇总---
#[cfg(feature = "std")]
pub use envelope::UuidIdGen;
pub use envelope::{
    actor_id, envelope_id, is_sealed, make_envelope, make_envelope_with_id, ActorId, Address,
    Completion, Envelope, EnvelopeBuildError, EnvelopeId, Header, IdGen, MakeEnvelope, SeqIdGen,
    MAX_PAYLOAD_BYTES,
};
pub use errors::{
    AmbiguousRoute, BackpressureError, BusError, ClockError, DeliveryError, EbaError, InvalidTopic,
    LeasePending, MailboxBusy, MailboxFull, RecordSinkError, SeqExhausted, WakeError, REENTRANT,
};
pub use job::{
    Call, Caller, CompletionSlot, Context, Effect, FinishJob, HostOptions, Intent, Job, JobHooks,
    JobHost, JobState, Lease, LeaseOptions, LeaseRegistry, Mailbox, Step,
};
pub use mail::{Bus, Inbox, Receiver, TrustPolicy};
pub use notify::{Agenda, Interest, Signal, Watchers, DELIVERY, READ, TIMER, WORK};
pub use pattern::{parse_pattern, split_topic, Pattern, Wildcard};
#[cfg(feature = "std")]
pub use pool::WorkerToken;
pub use pool::{PoolAbortedError, PoolClosedError, PoolError, PoolQueueFullError, WorkerPool};
pub use record::{
    dedup_key, DedupSink, DedupWindow, MemorySink, Record, RecordLog, RecordRing, RecordSink,
    Sequencer, RECORD_VERSION,
};
pub use runtime::{Drive, HostStats, Pending, Runtime, RuntimeStats};
pub use schema::{SchemaRegistry, TopicSchema};
#[cfg(feature = "std")]
pub use time::MonotonicClock;
pub use time::{
    pin_clock, safe_deadline, Clock, Deadlines, ManualClock, Schedule, Wake, WakeTicket,
    MAX_TIMER_DELAY_MS,
};
pub use value::{
    err_payload, err_payload_code, err_payload_code_detail, err_payload_detail, has_result_shape,
    is_result_err, is_result_ok, ok_payload, result_error, result_value, Value,
};
