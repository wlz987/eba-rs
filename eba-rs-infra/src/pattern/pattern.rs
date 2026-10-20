use crate::errors::InvalidTopic;
use crate::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Wildcard {
    None,
    Star,
    Globstar,
}

#[derive(Debug, Clone)]
pub struct Pattern {
    pub literal: Vec<String>,
    pub wildcard: Wildcard,
}

fn is_name(segment: &str) -> bool {
    if segment.is_empty() {
        return false;
    }
    let mut chars = segment.chars();
    let first = chars.next().unwrap();
    if !first.is_ascii_lowercase() {
        return false;
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

pub fn parse_pattern(text: &str) -> Result<Pattern, InvalidTopic> {
    if text.is_empty() {
        return Err(InvalidTopic("empty pattern".into()));
    }
    let parts: Vec<&str> = text.split('.').collect();
    if parts.iter().any(|part| part.is_empty()) {
        return Err(InvalidTopic(format!("illegal pattern: {:?}", text)));
    }
    for (index, part) in parts.iter().enumerate() {
        if *part == "*" || *part == "**" {
            if index != parts.len() - 1 {
                return Err(InvalidTopic(format!("wildcard not terminal: {:?}", text)));
            }
            continue;
        }
        if !is_name(part) {
            return Err(InvalidTopic(format!("illegal pattern segment: {:?}", part)));
        }
    }
    let mut literal: Vec<String> = parts.iter().map(|s| s.to_string()).collect();
    let wildcard = match parts.last() {
        Some(last) if *last == "*" => {
            literal.pop();
            Wildcard::Star
        }
        Some(last) if *last == "**" => {
            literal.pop();
            Wildcard::Globstar
        }
        _ => Wildcard::None,
    };
    Ok(Pattern { literal, wildcard })
}

pub fn lookup_keys(parts: &[String]) -> Vec<(Wildcard, String)> {
    let mut keys = vec![(Wildcard::None, parts.join("."))];
    if !parts.is_empty() {
        keys.push((Wildcard::Star, parts[..parts.len() - 1].join(".")));
    }
    keys.push((Wildcard::Globstar, String::new()));
    let mut prefix = String::new();
    for part in parts {
        if prefix.is_empty() {
            prefix = part.clone();
        } else {
            prefix = format!("{}.{}", prefix, part);
        }
        keys.push((Wildcard::Globstar, prefix.clone()));
    }
    keys
}
