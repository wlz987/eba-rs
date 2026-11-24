//! WorkerPool：有界并发提交。
//! std 版用 std::thread + mpsc 做受控 CPU/IO；no_std 版提供同步提交接口。
use crate::errors::{EbaError, Result};
/// 池错误：PoolClosedError / PoolQueueFullError / PoolAbortedError。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PoolError {
    PoolClosedError,
    PoolQueueFullError,
    PoolAbortedError,
}
impl PoolError {
    /// pool closed`）。
    pub fn class_name(&self) -> &'static str {
        match self {
            PoolError::PoolClosedError => "PoolClosedError",
            PoolError::PoolQueueFullError => "PoolQueueFullError",
            PoolError::PoolAbortedError => "PoolAbortedError",
        }
    }
}
impl core::fmt::Display for PoolError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // 各错误类的默认 message。
        let message = match self {
            PoolError::PoolClosedError => "pool closed",
            PoolError::PoolQueueFullError => "pool queue full",
            PoolError::PoolAbortedError => "pool aborted",
        };
        write!(f, "{}", message)
    }
}
#[cfg(feature = "std")]
pub struct WorkerToken {
    done: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
#[cfg(feature = "std")]
impl WorkerToken {
    pub fn is_done(&self) -> bool {
        self.done.load(std::sync::atomic::Ordering::Acquire)
    }
}
/// std 版：有界线程池。
#[cfg(feature = "std")]
pub struct WorkerPool {
    max: usize,
    running: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
#[cfg(feature = "std")]
impl WorkerPool {
    pub fn new(concurrency: usize) -> Self {
        Self {
            max: concurrency.max(1),
            running: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            closed: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
    pub fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::Acquire)
    }
    /// 提交任务；超过并发上限返回 PoolQueueFull。
    pub fn submit<F>(&self, task: F) -> Result<WorkerToken>
    where
        F: FnOnce() + Send + 'static,
    {
        if self.is_closed() {
            return Err(EbaError::Pool(PoolError::PoolClosedError));
        }
        let running = self
            .running
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if running >= self.max {
            self.running
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            return Err(EbaError::Pool(PoolError::PoolQueueFullError));
        }
        let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let done_clone = done.clone();
        let running_clone = self.running.clone();
        std::thread::spawn(move || {
            task();
            done_clone.store(true, std::sync::atomic::Ordering::Release);
            running_clone.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        });
        Ok(WorkerToken { done })
    }
    pub fn close(&self) {
        self.closed
            .store(true, std::sync::atomic::Ordering::Release);
    }
}
/// no_std 版：无线程，同步执行提交的任务（concurrency 退化为 1）。
#[cfg(not(feature = "std"))]
pub struct WorkerPool {
    closed: bool,
}
#[cfg(not(feature = "std"))]
impl WorkerPool {
    pub fn new(_concurrency: usize) -> Self {
        Self { closed: false }
    }
    pub fn is_closed(&self) -> bool {
        self.closed
    }
    pub fn submit<F: FnOnce()>(&self, task: F) -> Result<()> {
        if self.closed {
            return Err(EbaError::Pool(PoolError::PoolClosedError));
        }
        task();
        Ok(())
    }
    pub fn close(&mut self) {
        self.closed = true;
    }
}
/// 池错误类型名。
pub type PoolClosedError = PoolError;
pub type PoolQueueFullError = PoolError;
pub type PoolAbortedError = PoolError;
