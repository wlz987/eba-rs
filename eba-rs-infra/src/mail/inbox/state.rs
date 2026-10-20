use crate::prelude::*;
use alloc::collections::VecDeque;
use alloc::rc::{Rc, Weak};

use super::route::RouteRevoke;
use crate::envelope::Envelope;
use crate::notify::Signal;
use crate::notify::Watchers;

#[derive(Default)]
pub struct InboxState {
    pub items: VecDeque<Rc<Envelope>>,
    pub closed: bool,
    pub claimed: bool,
    pub work_paused: bool,
    pub read: Option<Rc<Signal>>,
    pub space: Option<Rc<Watchers>>,
    pub routes: Vec<Weak<RouteRevoke>>,
    pub seized: VecDeque<Rc<Envelope>>,
}
