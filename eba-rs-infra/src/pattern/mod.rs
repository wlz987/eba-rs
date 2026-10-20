#![allow(clippy::module_inception)]

pub mod pattern;
pub use pattern::{lookup_keys, parse_pattern, Pattern, Wildcard};
