//! Base record ring/log: ordered facts between producer and sink.

use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::vec::Vec;

use crate::envelope::Envelope;
use crate::errors::{BoxError, BusError};

pub const RECORD_VERSION: u8 = 1;

mod dedup;
pub use dedup::{dedup_key, DedupWindow};

#[derive(Clone)]
pub struct Record {
    pub v: u8,
    pub seq: u64,
    pub at: u64,
    pub envelope: Rc<Envelope>,
}

impl Record {
    pub fn new(seq: u64, at: u64, envelope: Rc<Envelope>) -> Self {
        Record {
            v: RECORD_VERSION,
            seq,
            at,
            envelope,
        }
    }
}

pub struct Sequencer {
    next: u64,
}

impl Sequencer {
    pub fn new(start: u64) -> Self {
        Self { next: start }
    }
    pub fn current(&self) -> u64 {
        self.next
    }
    pub fn next_seq(&mut self) -> u64 {
        let seq = self.next;
        self.next += 1;
        seq
    }
}

/// Bounded, preallocated ring. `try_push` never allocates; `shift` is O(1).
pub struct RecordRing {
    slots: Vec<Option<Record>>,
    head: usize,
    tail: usize,
    count: usize,
}

impl RecordRing {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "ring_capacity must be positive");
        RecordRing {
            slots: (0..capacity).map(|_| None).collect(),
            head: 0,
            tail: 0,
            count: 0,
        }
    }

    pub fn size(&self) -> usize {
        self.count
    }
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    pub fn is_full(&self) -> bool {
        self.count == self.slots.len()
    }

    pub fn try_push(&mut self, record: Record) -> bool {
        if self.is_full() {
            return false;
        }
        self.slots[self.tail] = Some(record);
        self.tail = (self.tail + 1) % self.slots.len();
        self.count += 1;
        true
    }

    pub fn shift(&mut self) -> Option<Record> {
        if self.is_empty() {
            return None;
        }
        let record = self.slots[self.head].take();
        self.head = (self.head + 1) % self.slots.len();
        self.count -= 1;
        record
    }

    pub fn drain(&mut self, limit: usize) -> Vec<Record> {
        let take = limit.min(self.count);
        let mut out = Vec::with_capacity(take);
        for _ in 0..take {
            if let Some(record) = self.shift() {
                out.push(record);
            }
        }
        out
    }

    pub fn clear(&mut self) {
        for slot in &mut self.slots {
            *slot = None;
        }
        self.head = 0;
        self.tail = 0;
        self.count = 0;
    }
}

pub trait RecordSink {
    fn append(&mut self, record: &Record) -> Result<(), BoxError>;
    fn flush(&mut self, seq: u64) -> Result<(), BoxError>;
    fn checkpoint(&mut self, seq: u64) -> Result<u64, BoxError>;
    fn durable(&self) -> u64;
    /// Wait until `seq` is durable. The built-in barrier path in `Core` is
    /// driven by the per-turn `flush_records` watermark; this hook exists so a
    /// custom durable log can observe/act on a barrier itself (ts parity).
    /// The default treats the barrier as satisfied once flushed.
    fn barrier(&mut self, seq: u64) -> Result<(), BoxError> {
        self.flush(seq)
    }
}

#[derive(Default)]
pub struct MemorySink {
    records: Vec<Record>,
    flushed: u64,
}

impl MemorySink {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn records(&self) -> &[Record] {
        &self.records
    }
}

impl RecordSink for MemorySink {
    fn append(&mut self, record: &Record) -> Result<(), BoxError> {
        if record.v != RECORD_VERSION {
            return Err(Box::from(BusError("unsupported record version".into())));
        }
        let last = self
            .records
            .last()
            .map(|item| item.seq)
            .unwrap_or(self.flushed);
        if record.seq <= last {
            return Err(Box::from(BusError("record out of order".into())));
        }
        self.records.push(record.clone());
        Ok(())
    }

    fn flush(&mut self, seq: u64) -> Result<(), BoxError> {
        let last = self.records.last().map(|item| item.seq).unwrap_or(0);
        if seq > last.max(self.flushed) {
            return Err(Box::from(BusError("flush beyond appended".into())));
        }
        if seq > self.flushed {
            self.flushed = seq;
        }
        Ok(())
    }

    fn checkpoint(&mut self, seq: u64) -> Result<u64, BoxError> {
        if seq > self.flushed {
            return Err(Box::from(BusError("checkpoint before flush".into())));
        }
        self.records.retain(|record| record.seq > seq);
        Ok(seq)
    }

    fn durable(&self) -> u64 {
        self.flushed
    }
}

/// Seq assignment + bounded ring on the scheduler side; sink on the worker side.
/// The sink is held as an object so a `Core` can be handed a custom durable
/// log (e.g. `std::fs`-backed) while still defaulting to `MemorySink`.
///
/// A delegating blanket impl makes `Box<dyn RecordSink>` itself a valid
/// `RecordSink`, so users can either pass a concrete `impl RecordSink` or
/// pre-box a trait object; both work.
pub struct RecordLog {
    sequencer: Sequencer,
    ring: RecordRing,
    sink: Box<dyn RecordSink>,
}

impl<T: RecordSink + ?Sized> RecordSink for Box<T> {
    fn append(&mut self, record: &Record) -> Result<(), BoxError> {
        self.as_mut().append(record)
    }
    fn flush(&mut self, seq: u64) -> Result<(), BoxError> {
        self.as_mut().flush(seq)
    }
    fn checkpoint(&mut self, seq: u64) -> Result<u64, BoxError> {
        self.as_mut().checkpoint(seq)
    }
    fn durable(&self) -> u64 {
        self.as_ref().durable()
    }
    fn barrier(&mut self, seq: u64) -> Result<(), BoxError> {
        self.as_mut().barrier(seq)
    }
}

impl RecordLog {
    pub fn new(sink: impl RecordSink + 'static, capacity: usize, start: u64) -> Self {
        RecordLog {
            sequencer: Sequencer::new(start),
            ring: RecordRing::new(capacity),
            sink: Box::new(sink),
        }
    }

    pub fn pending(&self) -> usize {
        self.ring.size()
    }
    pub fn capacity(&self) -> usize {
        self.ring.capacity()
    }
    pub fn durable(&self) -> u64 {
        self.sink.durable()
    }
    pub fn next_seq(&self) -> u64 {
        self.sequencer.current()
    }

    pub fn record(&mut self, envelope: Rc<Envelope>, at: u64) -> Option<Record> {
        if self.ring.is_full() {
            return None;
        }
        let record = Record::new(self.sequencer.next_seq(), at, envelope);
        self.ring.try_push(record.clone()).then_some(record)
    }

    pub fn drain(&mut self, limit: usize) -> Result<usize, BoxError> {
        let batch = self.ring.drain(limit);
        let moved = batch.len();
        for record in &batch {
            self.sink.append(record)?;
        }
        Ok(moved)
    }

    pub fn flush(&mut self, seq: u64) -> Result<(), BoxError> {
        self.sink.flush(seq)
    }
    /// Barrier: delegate to the sink. The `Core`-level deferred-barrier path
    /// still drives job resume; this hook is for sinks that want to observe.
    pub fn barrier(&mut self, seq: u64) -> Result<(), BoxError> {
        self.sink.barrier(seq)
    }
    pub fn checkpoint(&mut self, seq: u64) -> Result<u64, BoxError> {
        self.sink.checkpoint(seq)
    }
}
