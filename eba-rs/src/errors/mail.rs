use std::fmt;

#[derive(Debug)]
pub struct BusError(pub String);
impl fmt::Display for BusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for BusError {}

#[derive(Debug)]
pub struct MailboxFull(pub String);
impl fmt::Display for MailboxFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for MailboxFull {}
impl From<MailboxFull> for BusError {
    fn from(value: MailboxFull) -> Self {
        BusError(value.0)
    }
}

#[derive(Debug)]
pub struct InvalidTopic(pub String);
impl fmt::Display for InvalidTopic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for InvalidTopic {}
impl From<InvalidTopic> for BusError {
    fn from(value: InvalidTopic) -> Self {
        BusError(value.0)
    }
}

#[derive(Debug)]
pub struct BackpressureError(pub String);
impl fmt::Display for BackpressureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for BackpressureError {}

#[derive(Debug)]
pub struct EnvelopeBuildError(pub String);
impl fmt::Display for EnvelopeBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for EnvelopeBuildError {}
