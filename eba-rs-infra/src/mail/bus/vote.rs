use crate::prelude::*;
use alloc::rc::Rc;

use crate::envelope::Envelope;
use crate::errors::BoxError;

use super::lease::Lease;

pub enum PublishVote {
    Pass,
    Silent,
    Unreported,
    Fault(BoxError),
}

pub type PublishObserver = Rc<dyn Fn(&Rc<Envelope>) -> PublishVote>;

pub type GrantFrame = Vec<(usize, Rc<dyn Lease>)>;
