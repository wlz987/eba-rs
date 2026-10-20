use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::atomic::Ordering;

use super::core::Inbox;
use super::wakers::Wakers;
use super::CLOSES;
use crate::envelope::{Envelope, EnvelopeId};
use crate::notify::watch::Interest;
use crate::notify::{Signal, READ};

pub fn enqueue(inbox: &Rc<Inbox>, envelope: Rc<Envelope>) -> bool {
    if !can_enqueue(inbox, envelope.header.address.kind()) {
        return false;
    }
    let mut state = inbox.state.borrow_mut();
    state.items.push_back(envelope);
    true
}

pub fn can_enqueue(inbox: &Rc<Inbox>, kind: &str) -> bool {
    let state = inbox.state.borrow();
    !state.closed
        && state.items.len() < inbox.capacity
        && (!state.work_paused || (kind != "event" && kind != "request"))
}

pub fn pause_work(inbox: &Rc<Inbox>, paused: bool) {
    let mut wakers = Wakers::none();
    {
        let mut state = inbox.state.borrow_mut();
        if state.work_paused == paused {
            return;
        }
        state.work_paused = paused;
        if !paused {
            wakers = Wakers::of(&state);
            wakers.freed = true;
        }
    }
    wakers.run();
}

pub fn close_in_place(inbox: &Rc<Inbox>) -> bool {
    let mut state = inbox.state.borrow_mut();
    if state.closed {
        return false;
    }
    state.closed = true;
    CLOSES.fetch_add(1, Ordering::Relaxed);
    true
}

pub fn holds_kind(inbox: &Rc<Inbox>, kind: &str) -> bool {
    let state = inbox.state.borrow();
    state
        .items
        .iter()
        .any(|item| item.header.address.kind() == kind)
}

pub fn held_count(inbox: &Rc<Inbox>) -> usize {
    let state = inbox.state.borrow();
    state.items.len()
}

pub fn pull_kind(inbox: &Rc<Inbox>, kind: &str) -> Option<Rc<Envelope>> {
    let mut wakers = Wakers::none();
    let found = {
        let mut state = inbox.state.borrow_mut();
        let full = state.items.len() == inbox.capacity;
        let mut found = None;
        for index in 0..state.items.len() {
            if state.items[index].header.address.kind() != kind {
                continue;
            }
            found = state.items.remove(index);
            wakers = Wakers::of(&state);
            wakers.cleared = state.items.is_empty();
            wakers.freed = full;
            break;
        }
        found
    };
    wakers.run();
    found
}

pub fn find(inbox: &Rc<Inbox>, envelope_id: &EnvelopeId) -> Option<Rc<Envelope>> {
    let state = inbox.state.borrow();
    for item in &state.items {
        if item.header.id == *envelope_id {
            return Some(Rc::clone(item));
        }
    }
    None
}

pub fn remaining(inbox: &Inbox) -> usize {
    let state = inbox.state.borrow();
    if state.closed {
        return 0;
    }
    inbox.capacity - state.items.len()
}

pub fn mark_read(inbox: &Rc<Inbox>) {
    let read = inbox.state.borrow().read.clone();
    if let Some(read) = &read {
        read.notify(READ);
    }
}

pub fn read_signal(inbox: &Rc<Inbox>) -> Rc<Signal> {
    inbox.state.borrow().read.clone().expect("read signal")
}

pub fn watch_space(inbox: &Rc<Inbox>, interest: &Rc<Interest>) {
    if let Some(space) = &inbox.state.borrow().space {
        let space = space.clone();
        space.watch(interest);
    }
}

pub fn reply_topic(inbox: &Inbox) -> String {
    format!("sys.reply.i{}", inbox.id)
}

pub fn take_seized(inbox: &Rc<Inbox>) -> VecDeque<Rc<Envelope>> {
    let mut state = inbox.state.borrow_mut();
    std::mem::take(&mut state.seized)
}

pub struct InboxLedger {
    pub items: Vec<EnvelopeId>,
    pub seized: Vec<EnvelopeId>,
}

pub fn inspect(inbox: &Inbox) -> InboxLedger {
    let state = inbox.state.borrow();
    InboxLedger {
        items: state
            .items
            .iter()
            .map(|letter| letter.header.id.clone())
            .collect(),
        seized: state
            .seized
            .iter()
            .map(|letter| letter.header.id.clone())
            .collect(),
    }
}
