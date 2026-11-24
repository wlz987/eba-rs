//! 记录、去重与水位。
use crate::envelope::Envelope;
use crate::errors::{EbaError, Result};
use crate::prelude::*;
use crate::value::Value;
pub const RECORD_VERSION: u32 = 1;
#[derive(Clone, Debug)]
pub struct Record {
    /// 记录格式版本，供 sink 消费者校验落库版本。
    pub v: u32,
    /// 保留完整原始信封，sink 消费者可访问 header/shape。
    pub envelope: Envelope,
    pub topic: String,
    pub payload: Value,
    pub sv: u32,
    pub seq: u64,
    pub at: u64,
    pub dedup_key: String,
}
pub trait RecordSink {
    fn append(&mut self, record: &Record) -> Result<()>;
    fn flush(&mut self, up_to: u64) -> Result<()>;
    /// 首条记录前的序列空洞通知，默认无操作。
    fn gap(&mut self, _from: u64, _to: u64) {}
    /// 已落盘的水位。
    fn durable(&self) -> u64 {
        0
    }
    /// checkpoint：确认 seq 已落盘，之后可丢弃 ≤seq 的记录。默认恒等。
    fn checkpoint(&mut self, seq: u64) -> Result<u64> {
        Ok(seq)
    }
}
/// JS 安全整数上界：Sequencer 在 seq+1 越过 2^53-1 后置 null 并抛 SeqExhausted。
pub const MAX_SAFE_SEQ: u64 = (1u64 << 53) - 1;
/// 单调序号。
pub struct Sequencer {
    next: Option<u64>,
    last: u64,
}
impl Sequencer {
    pub fn new(start: u64) -> Self {
        Self {
            next: Some(start),
            // #last 固定初始化为 0，与 start 无关。
            last: 0,
        }
    }
    /// nextSeq()：耗尽时抛出可捕获的 SeqExhausted，而非 panic。
    pub fn try_next_seq(&mut self) -> Result<u64> {
        let n = self
            .next
            .ok_or(EbaError::SeqExhausted("seq exhausted".into()))?;
        self.next = if n == MAX_SAFE_SEQ { None } else { Some(n + 1) };
        self.last = n;
        Ok(n)
    }
    /// 兼容便捷入口：生产路径走 try_next_seq；直接调用且耗尽时 panic（B 类 panic 代 throw）。
    pub fn next_seq(&mut self) -> u64 {
        self.try_next_seq().expect("seq exhausted")
    }
    pub fn last(&self) -> u64 {
        self.last
    }
    /// 耗尽后回退到 lastSeq（#next ?? #last）。
    pub fn current(&self) -> u64 {
        self.next.unwrap_or(self.last)
    }
}
/// 有界环形缓冲（VecDeque 承载；cap 显式记录，不依赖可能预分配更大的 capacity()）。
pub struct RecordRing {
    cap: usize,
    items: VecDeque<Record>,
}
impl RecordRing {
    pub fn new(capacity: usize) -> Self {
        Self {
            cap: capacity,
            items: VecDeque::new(),
        }
    }
    pub fn capacity(&self) -> usize {
        self.cap
    }
    pub fn pending(&self) -> usize {
        self.items.len()
    }
    /// 满时返回 false，不驱逐（日志是唯一恢复事实）。
    pub fn push(&mut self, record: Record) -> bool {
        if self.items.len() >= self.cap {
            return false;
        }
        self.items.push_back(record);
        true
    }
    pub fn drain(&mut self) -> Vec<Record> {
        self.items.drain(..).collect()
    }
    pub fn peek(&self) -> Option<&Record> {
        self.items.front()
    }
    pub fn shift(&mut self) -> Option<Record> {
        self.items.pop_front()
    }
}
/// 去重滑动窗口：同一 (from,id) 在窗口内只落地一次。
pub struct DedupWindow {
    seen: BTreeSet<String>,
    order: VecDeque<String>,
    capacity: usize,
}
impl DedupWindow {
    pub fn new(capacity: usize) -> Self {
        Self {
            seen: BTreeSet::new(),
            order: VecDeque::new(),
            capacity,
        }
    }
    pub fn has(&self, key: &str) -> bool {
        self.seen.contains(key)
    }
    /// 返回是否首次接纳（重复返回 false）。
    pub fn accept(&mut self, key: impl Into<String>) -> bool {
        let key = key.into();
        if self.seen.contains(&key) {
            return false;
        }
        // 先加入再循环驱逐到 size <= capacity。
        // 容量 0 时新 key 加入后立即被驱逐，最终不保留任何 key。
        self.seen.insert(key.clone());
        self.order.push_back(key);
        while self.seen.len() > self.capacity {
            if let Some(old) = self.order.pop_front() {
                self.seen.remove(&old);
            } else {
                break;
            }
        }
        true
    }
}
/// 长度前缀键，避免
/// ("a:b","c") 与 ("a","b:c") 在裸 `from:id` 下碰撞导致记录误丢。
pub fn dedup_key(from: &str, id: &str) -> String {
    let mut out = String::with_capacity(from.len() + id.len() + 16);
    out.push_str(&from.len().to_string());
    out.push(':');
    out.push_str(from);
    out.push_str(&id.len().to_string());
    out.push(':');
    out.push_str(id);
    out
}
/// 内存 sink：测试与示例用，不是可靠存储。
pub struct MemorySink {
    log: Vec<Record>,
    flushed: u64,
}
impl MemorySink {
    pub fn new() -> Self {
        Self {
            log: Vec::new(),
            flushed: 0,
        }
    }
    pub fn durable(&self) -> u64 {
        self.flushed
    }
    pub fn len(&self) -> usize {
        self.log.len()
    }
    pub fn is_empty(&self) -> bool {
        self.log.is_empty()
    }
    /// MemorySink.records()：返回当前日志快照（防御性拷贝，调用方无法改内部）。
    pub fn records(&self) -> Vec<Record> {
        self.log.clone()
    }
}
impl Default for MemorySink {
    fn default() -> Self {
        Self::new()
    }
}
impl RecordSink for MemorySink {
    fn append(&mut self, record: &Record) -> Result<()> {
        // last = log 尾部或 flushed 水位（checkpoint 清空 log 后仍以 flushed 为下界）。
        let last = self.log.last().map(|r| r.seq).unwrap_or(self.flushed);
        if record.seq <= last {
            return Err(EbaError::RecordSink(format!(
                "record out of order: {} <= {}",
                record.seq, last
            )));
        }
        self.log.push(record.clone());
        Ok(())
    }
    fn flush(&mut self, up_to: u64) -> Result<()> {
        // appended = max(最后一条记录的 seq, flushed)。
        // checkpoint 清空 log 后，水位仍以 flushed 为准——不能越过已追加/已落盘的上界。
        let last = self.log.last().map(|r| r.seq).unwrap_or(0);
        let appended = core::cmp::max(last, self.flushed);
        if up_to > appended {
            return Err(EbaError::RecordSink(format!(
                "flush beyond appended: {} > {}",
                up_to, appended
            )));
        }
        if up_to > self.flushed {
            self.flushed = up_to;
        }
        Ok(())
    }
    fn durable(&self) -> u64 {
        self.flushed
    }
    fn checkpoint(&mut self, seq: u64) -> Result<u64> {
        if seq > self.flushed {
            return Err(crate::errors::EbaError::RecordSink(format!(
                "checkpoint before flush: {} > {}",
                seq, self.flushed
            )));
        }
        self.log.retain(|r| r.seq > seq);
        Ok(seq)
    }
}
/// 记录日志：封装、入环、排空、推进 durable 水位。
pub struct RecordLog {
    sink: Box<dyn RecordSink>,
    ring: RecordRing,
    seq: Sequencer,
    start: u64,
    gap_reported: bool,
}
impl RecordLog {
    pub fn new(sink: Box<dyn RecordSink>, capacity: usize, start: u64) -> Self {
        Self {
            sink,
            ring: RecordRing::new(capacity),
            seq: Sequencer::new(start),
            start,
            gap_reported: false,
        }
    }
    pub fn pending(&self) -> usize {
        self.ring.pending()
    }
    pub fn next_seq(&self) -> u64 {
        // RecordLog.nextSeq 返回 sequencer.current（start>1 且尚未 record 时
        // 不是 last+1）。
        self.seq.current()
    }
    pub fn last_seq(&self) -> u64 {
        self.seq.last()
    }
    pub fn durable(&self) -> u64 {
        // 实时读 sink 水位，不在 RecordLog 缓存。
        self.sink.durable()
    }
    /// 等待目标 seq 落盘；超界立即失败。
    /// 同步模型下没有 waiter 队列，调用方通过 `durable()` 轮询。
    pub fn barrier(&mut self, up_to: u64) -> Result<()> {
        if up_to > self.last_seq() {
            return Err(EbaError::Other(format!(
                "barrier target beyond recorded seq: {} > {}",
                up_to,
                self.last_seq()
            )));
        }
        // 207-220：先 drain staging 到 sink，再校验目标已落盘。
        self.drain()?;
        if up_to > self.durable() {
            return Err(EbaError::Other(format!(
                "barrier target not durable: {} > {}",
                up_to,
                self.durable()
            )));
        }
        Ok(())
    }
    /// 封装信封入环；环满返回 None。
    /// 兼容入口：环满返回 None；序号耗尽也返回 None（生产路径请用 record_checked）。
    pub fn record(&mut self, envelope: &Envelope, at: u64) -> Option<Record> {
        self.record_checked(envelope, at).ok().flatten()
    }
    /// 可捕获版本：环满返回 Ok(None)，序号耗尽返回 Err(SeqExhausted)。
    pub fn record_checked(&mut self, envelope: &Envelope, at: u64) -> Result<Option<Record>> {
        if !self.gap_reported {
            self.gap_reported = true;
            if self.start > 1 {
                self.sink.gap(1, self.start);
            }
        }
        if self.ring.pending() == self.ring.capacity() {
            return Ok(None);
        }
        let seq = self.seq.try_next_seq()?;
        let key = dedup_key(&envelope.header.from, &envelope.header.id);
        let record = Record {
            v: RECORD_VERSION,
            envelope: envelope.clone(),
            topic: envelope.header.topic.clone(),
            payload: envelope.payload.clone(),
            sv: envelope.header.sv,
            seq,
            at,
            dedup_key: key,
        };
        self.ring.push(record.clone());
        Ok(Some(record))
    }
    /// 把环里的 record 交给 sink.append，返回数量。
    /// append 失败不吞错、不丢记录——peek→append→shift，
    /// 失败时记录仍在环中。
    pub fn drain(&mut self) -> Result<usize> {
        let mut n = 0;
        while let Some(record) = self.ring.peek().cloned() {
            self.sink.append(&record)?;
            self.ring.shift();
            n += 1;
        }
        Ok(n)
    }
    /// 推进 durable 水位到 target（sink.flush 成功）。
    pub fn flush(&mut self, target: u64) -> Result<()> {
        self.sink.flush(target)?;
        Ok(())
    }
    /// 每轮：drain 后把水位推进到 lastSeq。
    pub fn settle(&mut self) -> Result<bool> {
        let target = self.last_seq();
        let durable = self.durable();
        if self.pending() == 0 && target <= durable {
            return Ok(false);
        }
        self.drain()?;
        if target > self.durable() {
            self.flush(target)?;
            return Ok(true);
        }
        Ok(false)
    }
    /// checkpoint：确认 seq 落盘并让 sink 丢弃旧记录。
    pub fn checkpoint(&mut self, seq: u64) -> Result<u64> {
        self.sink.checkpoint(seq)
    }
}
/// 去重 sink：装饰另一个 sink，同一 dedup key 只落地一次。
pub struct DedupSink {
    inner: Box<dyn RecordSink>,
    seen: DedupWindow,
}
impl DedupSink {
    pub fn new(inner: Box<dyn RecordSink>, capacity: usize) -> Self {
        Self {
            inner,
            seen: DedupWindow::new(capacity),
        }
    }
    pub fn inner(&self) -> &dyn RecordSink {
        self.inner.as_ref()
    }
}
impl RecordSink for DedupSink {
    fn gap(&mut self, from: u64, to: u64) {
        // 空洞通知必须透传给内层，不能吞。
        self.inner.gap(from, to);
    }
    fn append(&mut self, record: &Record) -> Result<()> {
        if self.seen.has(&record.dedup_key) {
            return Ok(());
        }
        self.inner.append(record)?;
        self.seen.accept(record.dedup_key.clone());
        Ok(())
    }
    fn flush(&mut self, up_to: u64) -> Result<()> {
        self.inner.flush(up_to)
    }
    fn durable(&self) -> u64 {
        self.inner.durable()
    }
    fn checkpoint(&mut self, seq: u64) -> Result<u64> {
        self.inner.checkpoint(seq)
    }
}
