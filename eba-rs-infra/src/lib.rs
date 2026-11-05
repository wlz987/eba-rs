#![forbid(unsafe_code)]
#![cfg_attr(not(feature = "std"), no_std)]

#[macro_use]
extern crate alloc;

mod prelude {
    pub use alloc::boxed::Box;
    pub use alloc::string::{String, ToString};
    pub use alloc::vec::Vec;
}

#[cfg(feature = "drive")]
pub mod drive;
#[cfg(feature = "drive")]
mod wake;

pub mod collections;
pub mod envelope;
pub mod errors;
pub mod job;
pub mod mail;
pub mod notify;
pub mod park;
pub mod pattern;
pub mod pool;
pub mod record;
pub mod rendezvous;
pub mod runtime;
pub mod schema;
pub mod time;

pub use park::Park;

pub type Payload = alloc::sync::Arc<dyn core::any::Any>;
