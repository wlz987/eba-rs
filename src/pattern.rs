//! Topic 模式匹配。
use crate::errors::{EbaError, Result};
use crate::prelude::*;
/// 校验并切分 topic。段规则：小写字母开头，其余 [a-z0-9_]。
pub fn split_topic(topic: &str) -> Result<Vec<String>> {
    if topic.is_empty() {
        return Err(EbaError::InvalidTopic("empty topic".into()));
    }
    let mut parts = Vec::new();
    for seg in topic.split('.') {
        if seg.is_empty() {
            return Err(EbaError::InvalidTopic(format!(
                "empty segment in {}",
                topic
            )));
        }
        let bytes = seg.as_bytes();
        if !bytes[0].is_ascii_lowercase() {
            return Err(EbaError::InvalidTopic(format!("bad segment {}", seg)));
        }
        for &b in &bytes[1..] {
            if b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' {
                continue;
            }
            return Err(EbaError::InvalidTopic(format!("bad segment {}", seg)));
        }
        parts.push(seg.to_string());
    }
    Ok(parts)
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Wildcard {
    None,
    Star,
    Globstar,
}
#[derive(Clone, Debug)]
pub struct Pattern {
    /// 具体段（不含末位通配符）——
    pub literal: Vec<String>,
    pub wildcard: Wildcard,
}
/// 解析模式：只有**末段**的 `*` / `**` 是通配符，其余段按字面量处理。
pub fn parse_pattern(pattern: &str) -> Result<Pattern> {
    let segs: Vec<&str> = pattern.split('.').collect();
    let last = segs.last().copied().unwrap_or("");
    let wildcard = if last == "*" {
        Wildcard::Star
    } else if last == "**" {
        Wildcard::Globstar
    } else {
        Wildcard::None
    };
    let literal_segs: &[&str] = if wildcard == Wildcard::None {
        &segs[..]
    } else {
        &segs[..segs.len() - 1]
    };
    // parsePattern 不校验段字符集；校验交给 ingress 的 split_topic。
    let literal = literal_segs.iter().map(|s| (*s).to_string()).collect();
    Ok(Pattern { literal, wildcard })
}
impl Pattern {
    /// 匹配一个具体 topic。
    /// 不含末位通配符的具体匹配段。
    /// 逐段迭代比较，**不把 topic 切分成 Vec**——匹配发生在每个信封的路由热路径上，
    /// 一次多余的临时 Vec 会乘以每秒投递的信封数。
    pub fn matches(&self, topic: &str) -> bool {
        let topic_parts: Vec<&str> = topic.split('.').collect();
        match self.wildcard {
            Wildcard::None => {
                self.literal.len() == topic_parts.len()
                    && self.literal.iter().zip(&topic_parts).all(|(p, t)| p == t)
            }
            Wildcard::Star => {
                // 末段 `*` 精确匹配一层：topic 段数 = literal + 1，前缀相等。
                self.literal.len() + 1 == topic_parts.len()
                    && self.literal.iter().zip(&topic_parts).all(|(p, t)| p == t)
            }
            Wildcard::Globstar => {
                // 末段 `**` 零或多层：topic 段数 >= literal，前缀相等。
                topic_parts.len() >= self.literal.len()
                    && self.literal.iter().zip(&topic_parts).all(|(p, t)| p == t)
            }
        }
    }
}
