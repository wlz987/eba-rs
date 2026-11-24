//! job 模块汇总。
// 子模块的 job 子模块
pub mod core;
pub mod effect;
#[allow(clippy::module_inception)]
pub mod job;
pub mod lease;
pub mod lease_registry;
pub mod mailbox;
pub use core::{Core, HostState};
pub use effect::{Effect, Intent};
pub use job::{
    Call, Caller, CompletionSlot, Context, FinishJob, HostOptions, Job, JobFactory, JobHooks,
    JobHost, JobState, Lifecycle, WaitHooks,
};
pub use lease::{Lease, LeaseOptions};
pub use lease_registry::LeaseRegistry;
pub use mailbox::{Mailbox, Step};
