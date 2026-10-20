use crate::prelude::*;
use core::fmt;

mod delivery;
mod mail;

pub use self::delivery::{
    delivery_envelope, is_lease_pending, is_outbox_full, DeliveryError, LeasePending,
};
pub use self::mail::{BackpressureError, BusError, EnvelopeBuildError, InvalidTopic, MailboxFull};

#[derive(Debug)]
pub struct MultipleFailures(pub Vec<Box<dyn core::error::Error + Send + Sync>>);
impl fmt::Display for MultipleFailures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "multiple failures")
    }
}
impl core::error::Error for MultipleFailures {}

pub type BoxError = Box<dyn core::error::Error + Send + Sync>;

pub const OUTBOX_FULL: &str = "result outbox full";
pub const REENTRANT: &str = "reentrant host execution";

#[derive(Debug)]
pub struct WakeError(pub String);
impl fmt::Display for WakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl core::error::Error for WakeError {}

#[derive(Debug)]
pub struct Settled(pub BoxError);
impl fmt::Display for Settled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl core::error::Error for Settled {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        Some(&*self.0)
    }
}

pub fn note_settled(error: BoxError) -> BoxError {
    if error.downcast_ref::<DeliveryError>().is_some()
        || error.downcast_ref::<WakeError>().is_some()
    {
        return error;
    }
    mark_settled(error)
}

pub fn mark_settled(error: BoxError) -> BoxError {
    Box::new(Settled(error))
}

pub fn is_settled(error: &BoxError) -> bool {
    error.downcast_ref::<Settled>().is_some()
}

pub fn bare<T: core::error::Error + Send + Sync + 'static>(error: &BoxError) -> Option<&T> {
    match error.downcast_ref::<Settled>() {
        Some(settled) => settled.0.downcast_ref::<T>(),
        None => error.downcast_ref::<T>(),
    }
}

pub fn combine_failures(original: BoxError, secondary: BoxError) -> BoxError {
    let mut chosen = None;
    if let Some(delivery) = secondary.downcast_ref::<DeliveryError>() {
        chosen = Some((
            delivery.envelope.clone(),
            delivery.reason.clone(),
            delivery.outbox_full,
        ));
    } else if let Some(delivery) = original.downcast_ref::<DeliveryError>() {
        chosen = Some((
            delivery.envelope.clone(),
            delivery.reason.clone(),
            delivery.outbox_full,
        ));
    }
    let grouped = MultipleFailures(vec![original, secondary]);
    if let Some((envelope, reason, outbox_full)) = chosen {
        let error = DeliveryError {
            envelope,
            reason,
            source: Some(Box::new(grouped)),
            outbox_full,
        };
        Box::new(error)
    } else {
        Box::new(grouped)
    }
}
