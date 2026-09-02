use crate::envelope::EnvelopeId;
use crate::job::Job;
use std::collections::HashMap;

#[derive(Default)]
pub struct SlotBook {
    by_id: HashMap<EnvelopeId, Job>,
    cause_index: HashMap<EnvelopeId, EnvelopeId>,
    /// 子请求 id → 发起 Job 槽位键(其根 id):结果按子请求 id 精确认亲(叶子槽位)。
    child_index: HashMap<EnvelopeId, EnvelopeId>,
}

impl SlotBook {
    pub fn new() -> SlotBook {
        SlotBook::default()
    }

    fn occupied_root_causes(&self) -> std::collections::HashSet<&EnvelopeId> {
        let mut occupied = std::collections::HashSet::new();
        for job in self.by_id.values() {
            let hdr = &job.root.header;
            if hdr.id == hdr.cause {
                occupied.insert(&hdr.cause);
            }
        }
        occupied
    }

    pub fn adopt(&mut self, job: Job) -> crate::Result<()> {
        let key = job.root.header.id.clone();
        if self.by_id.contains_key(&key) {
            return Err(crate::Error::State(format!("duplicate job key: {key:?}")));
        }
        let hdr = job.root.header.clone();
        if hdr.id == hdr.cause {
            if self.occupied_root_causes().contains(&hdr.cause) {
                return Err(crate::Error::State(format!(
                    "duplicate job cause: {:?}",
                    hdr.cause
                )));
            }
            self.cause_index.insert(hdr.cause.clone(), key.clone());
        }
        self.by_id.insert(key, job);
        Ok(())
    }

    pub fn take(&mut self, key: &EnvelopeId) -> Option<Job> {
        let job = self.by_id.remove(key)?;
        let hdr_cause = job.root.header.cause.clone();
        if self.cause_index.get(&hdr_cause).is_some_and(|k| k == key) {
            self.cause_index.remove(&hdr_cause);
        }
        Some(job)
    }

    pub fn index_child(&mut self, child: &EnvelopeId, key: &EnvelopeId) {
        self.child_index.insert(child.clone(), key.clone());
    }

    pub fn child_key(&self, child: &EnvelopeId) -> Option<&EnvelopeId> {
        self.child_index.get(child)
    }

    fn remove_child_entries(&mut self, key: &EnvelopeId) {
        self.child_index.retain(|_, k| k != key);
    }

    pub fn place(&mut self, job: Job) {
        if job.finished {
            let hdr = job.root.header.id.clone();
            self.remove_child_entries(&hdr);
            return;
        }
        let hdr = job.root.header.clone();
        if hdr.id == hdr.cause && !self.cause_index.contains_key(&hdr.cause) {
            self.cause_index.insert(hdr.cause.clone(), hdr.id.clone());
        }
        self.by_id.insert(hdr.id, job);
    }

    pub fn active_key<'a>(&'a self, cause: &'a EnvelopeId) -> Option<&'a EnvelopeId> {
        if let Some(key) = self.cause_index.get(cause) {
            return Some(key);
        }
        if self.by_id.contains_key(cause) {
            return Some(cause);
        }
        None
    }

    pub fn active_job(&self, cause: &EnvelopeId) -> Option<&Job> {
        self.active_key(cause).and_then(|k| self.by_id.get(k))
    }

    pub fn active_keys(&self) -> Vec<EnvelopeId> {
        self.by_id.keys().cloned().collect()
    }
}
