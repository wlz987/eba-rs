use std::rc::Rc;
use std::sync::{Arc, Mutex};

use crate::collections::IndexedMap;
use tokio::sync::oneshot;

use crate::envelope::result::Completion;
use crate::envelope::Envelope;
use crate::errors::BoxError;
use crate::job::caller::Caller;
use crate::job::core::HostState;
use crate::job::host::JobHost;

pub(crate) fn shut_door(caller: &Rc<Caller>) -> bool {
    let core = caller.core();
    let held = core.borrow();
    matches!(held.state, HostState::Closing | HostState::Closed) || held.sub.inbox.is_closed()
}

pub(crate) struct Waiter {
    pub(crate) envelope: Rc<Envelope>,
}

#[derive(Default)]
pub(crate) struct Inner {
    pub(crate) waiters: IndexedMap<String, Waiter>,
    pub(crate) stops: Vec<(Rc<Caller>, String)>,
    pub(crate) retired: Arc<Mutex<Vec<String>>>,
}

pub struct Pending {
    pub envelope: Rc<Envelope>,
    pub ready: oneshot::Receiver<Result<Completion, BoxError>>,
}
