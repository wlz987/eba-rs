use crate::envelope::Envelope;
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Debug, Default)]
struct Inner {
    items: std::collections::VecDeque<Envelope>,
    closed: bool,
    reader: Option<crate::envelope::ActorId>,
}

#[derive(Debug)]
pub struct Inbox {
    id: u64,
    capacity: usize,
    inner: RefCell<Inner>,
}

pub type InboxHandle = Rc<Inbox>;

static INBOX_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl Inbox {
    pub fn new(capacity: usize) -> InboxHandle {
        assert!(capacity >= 1, "inbox: capacity must be >= 1");
        Rc::new(Inbox {
            id: INBOX_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1,
            capacity,
            inner: RefCell::new(Inner::default()),
        })
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn is_closed(&self) -> bool {
        self.inner.borrow().closed
    }

    pub fn note_reader(&self, actor: crate::envelope::ActorId) {
        let mut inner = self.inner.borrow_mut();
        if inner.reader.is_none() {
            inner.reader = Some(actor);
        }
    }

    fn remaining(&self) -> usize {
        let inner = self.inner.borrow();
        if inner.closed {
            0
        } else {
            self.capacity - inner.items.len()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.inner.borrow().items.is_empty()
    }

    pub fn try_enqueue(&self, e: Envelope) -> bool {
        let mut inner = self.inner.borrow_mut();
        if inner.closed || inner.items.len() >= self.capacity {
            return false;
        }
        inner.items.push_back(e);
        true
    }

    pub fn try_recv(&self) -> Option<Envelope> {
        self.inner.borrow_mut().items.pop_front()
    }

    pub fn try_drop_last(&self, e: &Envelope) -> bool {
        let mut inner = self.inner.borrow_mut();
        match inner.items.back() {
            Some(back) if back == e => {
                inner.items.pop_back();
                true
            }
            _ => false,
        }
    }

    pub(crate) fn has_room(&self) -> bool {
        self.remaining() >= 1
    }

    pub fn close(&self) {
        let mut inner = self.inner.borrow_mut();
        if inner.closed {
            return;
        }
        inner.closed = true;
        inner.items.clear();
    }
}
