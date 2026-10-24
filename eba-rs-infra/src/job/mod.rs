#![allow(clippy::module_inception)]

pub mod book;
pub mod call;
pub mod caller;
pub mod close;
pub mod context;
pub mod core;
pub mod delivery;
pub mod ending;
pub mod execution;
pub(crate) mod held;
pub mod hook;
pub mod host;
pub mod job;
pub mod outbox;
pub mod pump;
mod reply;
pub mod resume;
mod service;

pub use reply::Reply;

pub use book::{Continuation, RequestBook};
pub use caller::{settle, settled, Call, Caller};
pub use core::{Core, CoreOptions, HostState};
pub use host::{core_of, JobHost};
pub use job::{Context, Job, JobHooks, Lifecycle, SharedIdGen};
pub use outbox::Outbox;
pub use service::Service;
