use std::sync::atomic::AtomicUsize;

mod core;
mod ops;
mod receiver;
mod route;
mod routes;
mod state;
mod wakers;

pub use self::core::{close_count, Inbox};
pub use self::ops::InboxLedger;
pub use self::ops::{
    can_enqueue, close_in_place, enqueue, find, held_count, holds_kind, inspect, mark_read,
    pause_work, pull_kind, read_signal, remaining, remove, replace_topic, reply_topic, take_seized,
    watch_space,
};
pub use self::receiver::{Receiver, ReceiverBorrow};
pub use self::route::RouteRevoke;
pub use self::routes::{forget_route, remember_route, route_count};
pub use self::state::InboxState;

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
static CLOSES: AtomicUsize = AtomicUsize::new(0);
