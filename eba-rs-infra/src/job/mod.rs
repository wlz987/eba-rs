#![allow(clippy::module_inception)]

pub mod call;
pub mod caller;
pub mod close;
pub mod context;
pub mod core;
pub mod delivery;
mod effect;
pub mod ending;
pub mod execution;
mod finished;
pub(crate) mod held;
pub mod hook;
pub mod host;
pub mod job;
mod job_lease;
pub mod lease_registry;
pub mod mailbox;
pub mod parked_leases;
pub mod pump;
pub mod resume;
mod service;

pub use effect::{apply_effects, Effect, EffectHost};
pub use job_lease::JobLease;
pub use parked_leases::ParkedLeases;

pub use caller::{settle, settled, Call, Caller};
pub use core::{Core, CoreOptions, HostState};
pub use host::{core_of, JobHost};
pub use job::{Context, Job, JobHooks, Lifecycle, SharedIdGen};
pub use lease_registry::{Continuation, LeaseRegistry};
pub use mailbox::Mailbox;
pub use service::Service;
