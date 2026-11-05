#![forbid(unsafe_code)]

//! std facade over `eba-rs-infra`: the core plus the tokio `Drive`.
//! The no_std port uses `eba-rs-infra` directly.

pub use eba_rs_infra::*;
