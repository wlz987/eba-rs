use super::super::errors::EnvelopeBuildError;
use crate::prelude::*;

pub fn is_name(segment: &str) -> bool {
    if segment.is_empty() {
        return false;
    }
    let mut chars = segment.chars();
    let first = chars.next().unwrap();
    if !(first.is_ascii_lowercase()) {
        return false;
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

pub fn split_topic(topic: &str) -> Result<Vec<String>, EnvelopeBuildError> {
    if topic.is_empty() {
        return Err(EnvelopeBuildError("topic must be a nonempty string".into()));
    }
    let mut parts = Vec::new();
    for part in topic.split('.') {
        if !is_name(part) {
            return Err(EnvelopeBuildError(format!(
                "illegal topic segment: {:?}",
                part
            )));
        }
        parts.push(part.to_string());
    }
    Ok(parts)
}
