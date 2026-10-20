use crate::envelope::result::ResultBody;
use crate::job::job::{Control, Intent};

use super::Context;

pub struct Commit {
    pub requests: Vec<Intent>,
    pub reply: Option<ResultBody>,
    pub control: Option<Control>,
}

pub fn seal_context(ctx: &Context) -> Commit {
    let mut buffer = ctx.buffer.borrow_mut();
    buffer.id_gen = None;
    Commit {
        requests: buffer.requests.clone(),
        reply: buffer.reply.clone(),
        control: buffer.control.clone(),
    }
}
