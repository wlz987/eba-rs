use alloc::rc::Rc;

use crate::envelope::result::Completion;
use crate::envelope::Envelope;
use crate::job::call::settled;

/// A front-door call in flight: the root envelope plus a poll for its completion.
pub struct Pending {
    envelope: Rc<Envelope>,
}

impl Pending {
    pub(crate) fn new(envelope: Rc<Envelope>) -> Self {
        Pending { envelope }
    }

    pub fn envelope(&self) -> &Rc<Envelope> {
        &self.envelope
    }

    pub fn poll(&self) -> Option<Completion> {
        settled(&self.envelope)
    }
}
