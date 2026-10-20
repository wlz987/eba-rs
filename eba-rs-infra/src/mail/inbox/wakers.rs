use alloc::rc::Rc;

use super::state::InboxState;
use crate::notify::Watchers;
use crate::notify::{Signal, READ};

pub(super) struct Wakers {
    read: Option<Rc<Signal>>,
    space: Option<Rc<Watchers>>,
    pub(super) cleared: bool,
    pub(super) freed: bool,
}

impl Wakers {
    pub(super) fn none() -> Wakers {
        Wakers {
            read: None,
            space: None,
            cleared: false,
            freed: false,
        }
    }

    pub(super) fn of(state: &InboxState) -> Wakers {
        Wakers {
            read: state.read.clone(),
            space: state.space.clone(),
            cleared: false,
            freed: false,
        }
    }

    pub(super) fn run(self) {
        if self.cleared {
            if let Some(read) = &self.read {
                read.clear(READ);
            }
        }
        if self.freed {
            if let Some(space) = &self.space {
                space.notify();
            }
        }
    }
}
