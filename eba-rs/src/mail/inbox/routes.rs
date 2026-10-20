use std::rc::{Rc, Weak};

use super::{Inbox, RouteRevoke};

pub fn remember_route(inbox: &Rc<Inbox>, owner: &Rc<RouteRevoke>) {
    let mut state = inbox.state.borrow_mut();
    if state
        .routes
        .iter()
        .filter_map(Weak::upgrade)
        .any(|live| RouteRevoke::same(&live, owner))
    {
        return;
    }
    state.routes.push(Rc::downgrade(owner));
}

pub fn route_count(inbox: &Inbox) -> usize {
    inbox
        .state
        .borrow()
        .routes
        .iter()
        .filter(|weak| weak.upgrade().is_some())
        .count()
}

pub fn forget_route(inbox: &Rc<Inbox>, owner: &Rc<RouteRevoke>) {
    let mut state = inbox.state.borrow_mut();
    state.routes.retain(|weak| {
        weak.upgrade()
            .map(|live| !RouteRevoke::same(&live, owner))
            .unwrap_or(false)
    });
}
