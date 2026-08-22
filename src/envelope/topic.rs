
use crate::envelope::error::EnvelopeBuildError;

pub fn is_name(segment: &str) -> bool {
    let mut chars = segment.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

pub fn split_topic(topic: &str) -> Result<Vec<&str>, EnvelopeBuildError> {
    if topic.is_empty() {
        return Err(EnvelopeBuildError::new("empty topic"));
    }
    let parts: Vec<&str> = topic.split('.').collect();
    for part in &parts {
        if !is_name(part) {
            return Err(EnvelopeBuildError::new(format!(
                "illegal topic segment: {part:?}"
            )));
        }
    }
    Ok(parts)
}

pub fn topic_suffix(topic: &str) -> Result<&str, EnvelopeBuildError> {
    if topic.is_empty() {
        return Err(EnvelopeBuildError::new("empty topic"));
    }
    Ok(topic.rsplit('.').next().unwrap_or(topic))
}
