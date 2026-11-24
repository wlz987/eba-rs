//! Schema 版本与迁移。
use crate::envelope::{Envelope, Header};
use crate::errors::{EbaError, Result};
use crate::prelude::*;
use crate::value::Value;
/// migrator 收到来源版本。
pub type Migrator = fn(Value, u32) -> Result<Value>;
pub type Validator = fn(&Value) -> Result<()>;
pub struct TopicSchema {
    /// 由 SchemaRegistry::register 注入，供错误消息生成 `topic@version` 前缀。
    topic: String,
    current: u32,
    migrators: BTreeMap<u32, Migrator>,
    validator: Option<Validator>,
}
/// 取内部文本，供多层错误包装复用（避免 Display 的 `Name: ` 前缀重复）。
fn error_text(error: &EbaError) -> String {
    let text = error.to_string();
    text.split_once(": ")
        .map(|(_, msg)| msg.to_string())
        .unwrap_or(text)
}
impl TopicSchema {
    pub fn new(current: u32) -> Self {
        Self {
            topic: String::new(),
            current,
            migrators: BTreeMap::new(),
            validator: None,
        }
    }
    pub fn with_migrator(mut self, from: u32, m: Migrator) -> Self {
        self.migrators.insert(from, m);
        self
    }
    /// 自定义字段级校验器。
    pub fn with_validator(mut self, v: Validator) -> Self {
        self.validator = Some(v);
        self
    }
    pub fn current(&self) -> u32 {
        self.current
    }
    /// 校验 payload。TopicSchema.validate 由注册方负责，
    /// 没有内建的 object 类型检查；未注册 validator 即不校验（Ok）。
    /// 校验 payload 并返回当前 schema 版本。
    fn validate_raw(&self, payload: &Value) -> Result<()> {
        match &self.validator {
            Some(v) => v(payload),
            None => Ok(()),
        }
    }
    /// {reason}`。
    pub fn check(&self, payload: &Value) -> Result<u32> {
        self.validate_raw(payload).map_err(|e| {
            EbaError::Other(format!(
                "invalid {}@{}: {}",
                self.topic,
                self.current,
                error_text(&e)
            ))
        })?;
        Ok(self.current)
    }
    /// 单次迁移到 current 并对结果复验。
    pub fn migrate(&self, payload: Value, from: u32) -> Result<Value> {
        if from > self.current {
            return Err(EbaError::Other(format!(
                "unsupported future schema version for {}: {}",
                self.topic, from
            )));
        }
        if from == self.current {
            return Ok(payload);
        }
        let m = self.migrators.get(&from).ok_or_else(|| {
            EbaError::Other(format!(
                "no migration for {}@{} -> {}",
                self.topic, from, self.current
            ))
        })?;
        let migrated = m(payload, from).map_err(|e| {
            EbaError::Other(format!(
                "invalid {}@{} after migration: {}",
                self.topic,
                self.current,
                error_text(&e)
            ))
        })?;
        self.validate_raw(&migrated).map_err(|e| {
            EbaError::Other(format!(
                "invalid {}@{} after migration: {}",
                self.topic,
                self.current,
                error_text(&e)
            ))
        })?;
        Ok(migrated)
    }
}
pub struct SchemaRegistry {
    topics: BTreeMap<String, TopicSchema>,
}
impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            topics: BTreeMap::new(),
        }
    }
    /// 注册 schema；版本必须严格递增。
    /// 抛普通 Error，RS 返回可捕获 Err（panic=abort 下不可恢复）。
    pub fn register(&mut self, topic: impl Into<String>, mut schema: TopicSchema) -> Result<()> {
        let topic = topic.into();
        if let Some(existing) = self.topics.get(&topic) {
            if schema.current <= existing.current() {
                return Err(EbaError::Other(format!(
                    "schema {} is already at version {}",
                    topic,
                    existing.current()
                )));
            }
        }
        schema.topic = topic.clone();
        self.topics.insert(topic, schema);
        Ok(())
    }
    /// 未注册主题的默认版本：
    /// 无 schema 的邮件视为 sv=1，迁移到 current 时走正常 fail-closed 路径。
    pub fn version(&self, topic: &str) -> u32 {
        self.topics.get(topic).map(|s| s.current()).unwrap_or(1)
    }
    pub fn size(&self) -> usize {
        self.topics.len()
    }
    /// 返回匹配到的 schema 版本，未注册返回默认版本 1。
    pub fn check(&self, topic: &str, payload: &Value) -> Result<u32> {
        match self.topics.get(topic) {
            Some(s) => s.check(payload),
            None => Ok(1),
        }
    }
    /// 未注册 topic 直接拒绝（fail-closed），不放行 payload。
    pub fn migrate(&self, topic: &str, payload: Value, from: u32) -> Result<Value> {
        match self.topics.get(topic) {
            Some(s) => s.migrate(payload, from),
            None => Err(EbaError::Other(format!("no schema for {}", topic))),
        }
    }
    /// ingress：按 sv 逐级迁移信封 payload，失败即 fail-closed。
    /// 没有"sv==0 放行"的后门；当前版本直接返回，
    /// 其余一律走 migrate（缺 migrator / 超前版本 / 迁移失败都会拒绝）。
    pub fn migrate_envelope(&self, envelope: &Envelope) -> Result<Envelope> {
        let topic = &envelope.header.topic;
        let current = self.version(topic);
        let from = envelope.header.sv;
        if from == current {
            return Ok(envelope.clone());
        }
        let migrated = self.migrate(topic, envelope.payload.clone(), from)?;
        let mut header: Header = envelope.header.clone();
        header.sv = current;
        Ok(Envelope::from_parts(header, migrated, envelope.is_sealed()))
    }
}
impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}
