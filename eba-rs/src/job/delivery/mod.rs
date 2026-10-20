mod parking;
mod send;

use self::parking::{complete_held, park};
pub use self::parking::{
    holds_letter, next_sendable, note_delivery_failure, note_delivery_success,
    raise_delivery_failure, release_attempts, retry_due, Slot,
};
pub(crate) use self::send::{defer_ending, match_reply, remove};
pub use self::send::{hold_or_send, release_result};
