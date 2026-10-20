#![forbid(unsafe_code)]

pub mod collections;
pub mod drive;
pub mod envelope;
pub mod errors;
pub mod job;
pub mod mail;
pub mod notify;
pub mod pattern;
pub mod rendezvous;
pub mod runtime;
pub mod time;

mod wake;

pub type Payload = std::sync::Arc<dyn std::any::Any + Send + Sync>;
