use std::fmt;
use std::sync::Arc;

use crate::envelope::Envelope;

use super::{BoxError, OUTBOX_FULL};

#[derive(Debug)]
pub struct DeliveryError {
    pub envelope: Arc<Envelope>,
    pub reason: String,
    pub source: Option<Box<dyn std::error::Error>>,
    pub outbox_full: bool,
}

impl DeliveryError {
    pub fn new(envelope: Arc<Envelope>, reason: impl Into<String>) -> Self {
        DeliveryError {
            envelope,
            reason: reason.into(),
            source: None,
            outbox_full: false,
        }
    }

    pub fn caused(envelope: Arc<Envelope>, reason: impl Into<String>, source: BoxError) -> Self {
        DeliveryError {
            envelope,
            reason: reason.into(),
            source: Some(source),
            outbox_full: false,
        }
    }

    pub fn outbox_full(envelope: Arc<Envelope>) -> Self {
        DeliveryError {
            envelope,
            reason: OUTBOX_FULL.into(),
            source: None,
            outbox_full: true,
        }
    }
}

impl fmt::Display for DeliveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.reason)
    }
}
impl std::error::Error for DeliveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}

#[derive(Debug, Default)]
pub struct LeasePending {
    pub cause: Option<BoxError>,
}

impl LeasePending {
    pub fn new() -> Self {
        LeasePending { cause: None }
    }

    pub fn caused(cause: BoxError) -> Self {
        LeasePending { cause: Some(cause) }
    }
}

impl fmt::Display for LeasePending {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "lease release pending")
    }
}

impl std::error::Error for LeasePending {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause
            .as_deref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}

pub fn is_lease_pending(error: &BoxError) -> bool {
    error.downcast_ref::<LeasePending>().is_some()
}

pub fn is_outbox_full(error: &BoxError) -> bool {
    error
        .downcast_ref::<DeliveryError>()
        .is_some_and(|delivery| delivery.outbox_full)
}

pub fn delivery_envelope(error: &BoxError) -> Option<Arc<Envelope>> {
    error
        .downcast_ref::<DeliveryError>()
        .map(|delivery| delivery.envelope.clone())
}
