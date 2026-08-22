
use crate::envelope::id::EnvelopeId;
use crate::envelope::topic_segment;
use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

pub trait IdGen {
    fn next_envelope_id(&mut self) -> EnvelopeId;
    fn topic_segment(&self, id: &EnvelopeId) -> Result<String, crate::Error>;
}

pub type IdGenHandle = Rc<RefCell<dyn IdGen>>;

pub struct UuidIdGen {
    counter: u64,
}

impl Default for UuidIdGen {
    fn default() -> Self {
        Self::new()
    }
}

impl UuidIdGen {
    pub fn new() -> Self {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x9e3779b97f4a7c15);
        UuidIdGen {
            counter: seed | 1,
        }
    }

    fn splitmix(&mut self) -> u64 {
        self.counter = self.counter.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.counter;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
}

impl IdGen for UuidIdGen {
    fn next_envelope_id(&mut self) -> EnvelopeId {
        let a = self.splitmix();
        let b = self.splitmix();
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&a.to_be_bytes());
        bytes[8..].copy_from_slice(&b.to_be_bytes());
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        EnvelopeId(hex)
    }

    fn topic_segment(&self, id: &EnvelopeId) -> Result<String, crate::Error> {
        Ok(topic_segment(id)?)
    }
}

pub struct SeqIdGen {
    next: u64,
}

impl Default for SeqIdGen {
    fn default() -> Self {
        Self { next: 1 }
    }
}

impl SeqIdGen {
    pub fn new(start: u64) -> Self {
        SeqIdGen {
            next: start.max(1),
        }
    }
}

impl IdGen for SeqIdGen {
    fn next_envelope_id(&mut self) -> EnvelopeId {
        let n = self.next;
        self.next += 1;
        EnvelopeId(format!("{n:032x}"))
    }

    fn topic_segment(&self, id: &EnvelopeId) -> Result<String, crate::Error> {
        Ok(topic_segment(id)?)
    }
}

impl fmt::Debug for dyn IdGen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("IdGen")
    }
}
