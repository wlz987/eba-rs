use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use crate::envelope::{Envelope, EnvelopeId};
use crate::mail::bus::Bus;
use crate::mail::inbox::Inbox;
use crate::rendezvous::records::RecordsLedger;
use crate::rendezvous::records::{Claim, Records};

pub struct Continuation {
    pub owner: EnvelopeId,
    pub stage: String,
}

pub struct RequestBook {
    records: Records<Continuation>,
    owners: HashMap<EnvelopeId, HashSet<EnvelopeId>>,
    cancelled: Vec<EnvelopeId>,
    #[cfg(test)]
    fail_cancel: bool,
}

impl RequestBook {
    pub fn new() -> Self {
        RequestBook {
            records: Records::new(),
            owners: HashMap::new(),
            cancelled: Vec::new(),
            #[cfg(test)]
            fail_cancel: false,
        }
    }

    pub fn size(&self) -> usize {
        self.records.size()
    }

    pub fn next_deadline_ms(&self) -> Option<u64> {
        self.records.next_deadline_ms()
    }

    pub fn unreleased(&self) -> usize {
        self.records.unreleased()
    }

    pub fn count(&self, owner: &EnvelopeId) -> usize {
        self.owners.get(owner).map_or(0, |set| set.len())
    }

    pub fn holds(&self, request_id: &EnvelopeId) -> bool {
        self.records.continuation(request_id).is_some()
    }

    pub fn inspect(&self) -> RecordsLedger {
        self.records.inspect()
    }

    pub fn submit(
        &mut self,
        bus: &Bus,
        inbox: &Rc<Inbox>,
        request: Rc<Envelope>,
        owner: EnvelopeId,
        stage: String,
        deadline_ms: Option<u64>,
    ) -> Result<(), crate::errors::BoxError> {
        let continuation = Continuation {
            owner: owner.clone(),
            stage,
        };
        let request_id = request.header.id.clone();
        let result = self
            .records
            .submit(bus, inbox, request, continuation, deadline_ms);
        if self.records.continuation(&request_id).is_some() {
            self.owners.entry(owner).or_default().insert(request_id);
        }
        result
    }

    pub fn take_result(
        &mut self,
        envelope: &Rc<Envelope>,
    ) -> Result<Option<Claim<Continuation>>, crate::errors::BoxError> {
        let claim = self.records.take_result(envelope)?;
        Ok(self.claim(claim))
    }

    pub fn take_due(&mut self, now_ms: u64) -> Option<Claim<Continuation>> {
        let claim = self.records.take_due(now_ms);
        self.claim(claim)
    }

    pub fn elapse_open(&mut self) {
        self.records.elapse_open();
    }

    pub fn recover(&mut self) {
        self.records.recover();
    }

    #[cfg(test)]
    pub fn fail_next_cancel(&mut self) {
        self.fail_cancel = true;
    }

    pub fn cancel(&mut self, owner: &EnvelopeId) -> Result<(), crate::errors::BoxError> {
        #[cfg(test)]
        if self.fail_cancel {
            self.fail_cancel = false;
            return Err(Box::from("lease"));
        }
        let owned = self.owners.get(owner).cloned().unwrap_or_default();
        for request_id in owned {
            self.records.finish(&request_id, false)?;
            self.forget(owner, &request_id);
            self.cancelled.push(request_id);
        }
        Ok(())
    }

    pub fn take_cancelled(&mut self) -> Vec<EnvelopeId> {
        std::mem::take(&mut self.cancelled)
    }

    fn claim(&mut self, claim: Option<Claim<Continuation>>) -> Option<Claim<Continuation>> {
        let claim = claim?;
        let owner = claim.continuation.owner.clone();
        let request_id = claim.completion.request_id.clone();
        self.forget(&owner, &request_id);
        Some(claim)
    }

    fn forget(&mut self, owner: &EnvelopeId, request_id: &EnvelopeId) {
        if let Some(owned) = self.owners.get_mut(owner) {
            owned.remove(request_id);
            if owned.is_empty() {
                self.owners.remove(owner);
            }
        }
    }
}

impl Default for RequestBook {
    fn default() -> Self {
        Self::new()
    }
}
