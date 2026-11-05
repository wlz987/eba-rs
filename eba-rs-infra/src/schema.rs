//! Topic schema registry: versioned validation and fail-closed migration.

use crate::prelude::*;
use alloc::collections::BTreeMap;
use alloc::string::String;

use crate::errors::{BoxError, BusError};
use crate::Payload;

pub type Validate = Box<dyn Fn(&Payload) -> Result<(), String>>;
pub type Migrate = Box<dyn Fn(Payload, u64) -> Result<Payload, String>>;

pub struct TopicSchema {
    pub version: u64,
    pub validate: Validate,
    pub migrate: Option<Migrate>,
}

#[derive(Default)]
pub struct SchemaRegistry {
    topics: BTreeMap<String, TopicSchema>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, topic: &str, schema: TopicSchema) -> Result<(), BoxError> {
        if topic.is_empty() {
            return Err(Box::from(BusError("topic must be non-empty".into())));
        }
        if schema.version == 0 {
            return Err(Box::from(BusError(
                "schema_version must be positive".into(),
            )));
        }
        if let Some(existing) = self.topics.get(topic) {
            if schema.version <= existing.version {
                return Err(Box::from(BusError(format!(
                    "schema {topic} is already at version {}",
                    existing.version
                ))));
            }
        }
        self.topics.insert(topic.into(), schema);
        Ok(())
    }

    pub fn version(&self, topic: &str) -> u64 {
        self.topics.get(topic).map_or(1, |schema| schema.version)
    }

    pub fn check(&self, topic: &str, payload: &Payload) -> Result<u64, BoxError> {
        let Some(schema) = self.topics.get(topic) else {
            return Ok(1);
        };
        (schema.validate)(payload).map_err(|message| -> BoxError {
            Box::from(BusError(format!(
                "invalid {topic}@{}: {message}",
                schema.version
            )))
        })?;
        Ok(schema.version)
    }

    pub fn migrate(
        &self,
        topic: &str,
        payload: Payload,
        from_version: u64,
    ) -> Result<(u64, Payload), BoxError> {
        let schema = self
            .topics
            .get(topic)
            .ok_or_else(|| -> BoxError { Box::from(BusError(format!("no schema for {topic}"))) })?;
        if from_version == 0 {
            return Err(Box::from(BusError("invalid schema version: 0".into())));
        }
        if from_version > schema.version {
            return Err(Box::from(BusError(format!(
                "unsupported future schema version for {topic}: {from_version}"
            ))));
        }
        if from_version == schema.version {
            return Ok((schema.version, payload));
        }
        let Some(migrate) = &schema.migrate else {
            return Err(Box::from(BusError(format!(
                "no migration for {topic}@{from_version} -> {}",
                schema.version
            ))));
        };
        let migrated = migrate(payload, from_version).map_err(|message| -> BoxError {
            Box::from(BusError(format!(
                "invalid {topic}@{} after migration: {message}",
                schema.version
            )))
        })?;
        (schema.validate)(&migrated).map_err(|message| -> BoxError {
            Box::from(BusError(format!(
                "invalid {topic}@{} after migration: {message}",
                schema.version
            )))
        })?;
        Ok((schema.version, migrated))
    }
}
