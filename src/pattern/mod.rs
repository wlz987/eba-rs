
use std::format;
use std::vec::Vec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Wildcard {
    #[default]
    None,
    Star,
    GlobStar,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Pattern {
    pub literal: Vec<String>,
    pub wildcard: Wildcard,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidTopic(pub String);

impl std::fmt::Display for InvalidTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<InvalidTopic> for crate::Error {
    fn from(e: InvalidTopic) -> Self {
        crate::Error::InvalidTopic(e.0)
    }
}

pub fn parse_pattern(text: &str) -> Result<Pattern, InvalidTopic> {
    if text.is_empty() {
        return Err(InvalidTopic("empty pattern".into()));
    }
    let parts: Vec<&str> = text.split('.').collect();
    if parts.iter().any(|p| p.is_empty()) {
        return Err(InvalidTopic(format!("illegal pattern: {text:?}")));
    }
    let last = parts.len() - 1;
    for (index, part) in parts.iter().enumerate() {
        if *part == "*" || *part == "**" {
            if index != last {
                return Err(InvalidTopic(format!("wildcard not terminal: {text:?}")));
            }
            continue;
        }
        if !crate::envelope::is_name(part) {
            return Err(InvalidTopic(format!("illegal pattern segment: {part:?}")));
        }
    }
    let wildcard = match parts[last] {
        "*" => Wildcard::Star,
        "**" => Wildcard::GlobStar,
        _ => Wildcard::None,
    };
    let literal: Vec<String> = if wildcard == Wildcard::None {
        parts.iter().map(|s| (*s).into()).collect()
    } else {
        parts[..last].iter().map(|s| (*s).into()).collect()
    };
    Ok(Pattern { literal, wildcard })
}

pub fn matches(pattern: &Pattern, topic_parts: &[&str]) -> bool {
    let n = pattern.literal.len();
    match pattern.wildcard {
        Wildcard::None => topic_parts == pattern.literal.as_slice(),
        Wildcard::Star => topic_parts.len() == n + 1 && topic_parts[..n] == pattern.literal[..],
        Wildcard::GlobStar => topic_parts.len() >= n && topic_parts[..n] == pattern.literal[..],
    }
}
