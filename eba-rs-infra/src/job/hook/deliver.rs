use alloc::rc::Rc;

use crate::envelope::Envelope;
use crate::errors::{BoxError, DeliveryError};
use crate::job::core::Core;
use crate::job::delivery::hold_or_send;
use crate::job::ending::{leave_ending, settle_ending};
use crate::job::job::{Control, Job};
use crate::mail::bus::Bus;

pub fn deliver_ending(
    core: &mut Core,
    bus: &Bus,
    job: &Job,
    reply: Option<Rc<Envelope>>,
    control: Option<Control>,
) -> Result<(), BoxError> {
    if let Some(envelope) = reply {
        match hold_or_send(core, bus, Some(job.clone()), envelope) {
            Ok(accepted) => {
                job.state.borrow_mut().replied = true;
                if !accepted {
                    leave_ending(job, control);
                    return Ok(());
                }
            }
            Err(error) => {
                if error.downcast_ref::<DeliveryError>().is_some() {
                    job.state.borrow_mut().replied = true;
                    leave_ending(job, control);
                }
                return Err(error);
            }
        }
    }
    settle_ending(core, job, control)
}

pub(crate) fn delivery_envelope(error: &BoxError) -> Option<Rc<Envelope>> {
    error
        .downcast_ref::<DeliveryError>()
        .map(|delivery| Rc::new((*delivery.envelope).clone()))
}
