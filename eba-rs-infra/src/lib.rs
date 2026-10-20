#![forbid(unsafe_code)]
#![cfg_attr(not(feature = "std"), no_std)]

#[macro_use]
extern crate alloc;

mod prelude {
    pub use alloc::boxed::Box;
    pub use alloc::string::{String, ToString};
    pub use alloc::vec::Vec;
}

pub mod collections;
pub mod envelope;
pub mod errors;
pub mod job;
pub mod mail;
pub mod notify;
pub mod pattern;
pub mod rendezvous;
pub mod runtime;
pub mod time;

pub type Payload = alloc::sync::Arc<dyn core::any::Any + Send + Sync>;
