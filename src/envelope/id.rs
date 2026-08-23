use crate::envelope::error::EnvelopeBuildError;
use crate::envelope::topic::is_name;
use std::fmt;

const MAX_SEG: usize = 64;
const SEG_PREFIX: char = 'e';

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct EnvelopeId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct ActorId(pub String);

impl fmt::Display for EnvelopeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for EnvelopeId {
    fn from(s: &str) -> Self {
        EnvelopeId(s.into())
    }
}

impl From<&str> for ActorId {
    fn from(s: &str) -> Self {
        ActorId(s.into())
    }
}

pub fn topic_segment(id: &EnvelopeId) -> Result<String, EnvelopeBuildError> {
    let cleaned: String =
        id.0.chars()
            .filter(|c| *c != '-')
            .map(|c| c.to_ascii_lowercase())
            .collect();
    if cleaned.is_empty() || !cleaned.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(EnvelopeBuildError::new(format!(
            "invalid envelope id: {:?}",
            id.0
        )));
    }
    let segment = format!("{SEG_PREFIX}{cleaned}");
    if segment.len() > MAX_SEG || !is_name(&segment) {
        return Err(EnvelopeBuildError::new(format!(
            "invalid topic_segment for id: {:?}",
            id.0
        )));
    }
    Ok(segment)
}
