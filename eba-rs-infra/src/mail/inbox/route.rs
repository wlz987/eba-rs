use alloc::rc::Rc;

use super::Inbox;

type RevokeFn = Rc<dyn Fn(&Rc<Inbox>)>;

pub struct RouteRevoke {
    revoke: RevokeFn,
}

impl RouteRevoke {
    pub fn new(revoke: impl Fn(&Rc<Inbox>) + 'static) -> Rc<RouteRevoke> {
        Rc::new(RouteRevoke {
            revoke: Rc::new(revoke),
        })
    }

    pub fn unsubscribe_all(&self, inbox: &Rc<Inbox>) {
        (self.revoke)(inbox)
    }

    pub fn same(this: &Rc<RouteRevoke>, other: &Rc<RouteRevoke>) -> bool {
        Rc::ptr_eq(this, other)
    }
}
