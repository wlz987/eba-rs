//! Bounded worker pool. Tasks run when a slot is free; completion releases it.
//!
//! This is the no_std callback analog of ts `WorkerPool`: no Promise, but the
//! same concurrency, queue bound, abort-before-start and close semantics.

use crate::prelude::*;
use alloc::collections::VecDeque;
use alloc::rc::Rc;
use core::cell::RefCell;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolError {
    Closed,
    QueueFull,
    Aborted,
}

struct Inner {
    concurrency: usize,
    max_queue: usize,
    queue: VecDeque<Box<dyn FnOnce(WorkerToken)>>,
    active: usize,
    closed: bool,
}

pub struct WorkerPool {
    inner: Rc<RefCell<Inner>>,
}

pub struct WorkerToken {
    inner: Rc<RefCell<Inner>>,
    done: bool,
}

impl WorkerToken {
    pub fn complete(mut self) {
        self.finish();
    }

    fn finish(&mut self) {
        if self.done {
            return;
        }
        self.done = true;
        {
            let mut inner = self.inner.borrow_mut();
            if inner.active > 0 {
                inner.active -= 1;
            }
        }
        pump(&self.inner);
    }
}

impl Drop for WorkerToken {
    fn drop(&mut self) {
        self.finish();
    }
}

impl WorkerPool {
    pub fn new(concurrency: usize, max_queue: usize) -> Self {
        assert!(concurrency > 0, "pool_concurrency must be positive");
        WorkerPool {
            inner: Rc::new(RefCell::new(Inner {
                concurrency,
                max_queue,
                queue: VecDeque::new(),
                active: 0,
                closed: false,
            })),
        }
    }

    pub fn active_count(&self) -> usize {
        self.inner.borrow().active
    }

    pub fn queued_count(&self) -> usize {
        self.inner.borrow().queue.len()
    }

    pub fn is_closed(&self) -> bool {
        self.inner.borrow().closed
    }

    pub fn submit<F>(&self, aborted: bool, task: F) -> Result<(), PoolError>
    where
        F: FnOnce(WorkerToken) + 'static,
    {
        if self.inner.borrow().closed {
            return Err(PoolError::Closed);
        }
        if aborted {
            return Err(PoolError::Aborted);
        }
        {
            let mut inner = self.inner.borrow_mut();
            if inner.active >= inner.concurrency && inner.queue.len() >= inner.max_queue {
                return Err(PoolError::QueueFull);
            }
            inner.queue.push_back(Box::new(task));
        }
        pump(&self.inner);
        Ok(())
    }

    pub fn close(&self) {
        let mut inner = self.inner.borrow_mut();
        inner.closed = true;
        inner.queue.clear();
    }
}

fn pump(inner: &Rc<RefCell<Inner>>) {
    let starts: Vec<Box<dyn FnOnce(WorkerToken)>> = {
        let mut state = inner.borrow_mut();
        let mut starts = Vec::new();
        while !state.closed && state.active < state.concurrency && !state.queue.is_empty() {
            state.active += 1;
            if let Some(task) = state.queue.pop_front() {
                starts.push(task);
            }
        }
        starts
    };
    for task in starts {
        task(WorkerToken {
            inner: Rc::clone(inner),
            done: false,
        });
    }
}
