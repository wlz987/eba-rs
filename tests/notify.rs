//! Signal/Watchers/Interest/Agenda + WorkerPool。
use eba_rs::*;
use std::cell::RefCell;
use std::rc::Rc;
#[test]
fn signal_flags_bind_notify_clear_detach() {
    let s = Signal::new();
    assert!(!s.is_bound());
    s.bind(1, "actor");
    assert!(s.is_bound());
    assert!(s.bound_to(1));
    assert!(!s.bound_to(2));
    assert_eq!(s.actor().as_deref(), Some("actor"));
    assert!(!s.has(WORK));
    s.notify(WORK);
    assert!(s.has(WORK));
    s.clear(WORK);
    assert!(!s.has(WORK));
    s.set(TIMER, true);
    assert!(s.has(TIMER));
    s.detach();
    assert!(!s.is_bound());
}
#[test]
fn watchers_on_empty_and_notify() {
    let called = Rc::new(RefCell::new(0));
    let c = called.clone();
    let sig = Rc::new(Signal::new());
    sig.bind(1, "w");
    let interest = Rc::new(Interest::new(1, "t", sig.clone()));
    // onEmpty 用 Interest 的 hook 间接验证：notify 后调用 signal。
    let w = Watchers::new(1, None);
    w.watch(&interest);
    assert_eq!(w.watched(), 1);
    assert_eq!(w.live().len(), 1);
    w.notify();
    assert!(sig.has(DELIVERY), "interest default notify sets DELIVERY");
    assert_eq!(w.watched(), 0);
    w.remove(&interest);
    assert_eq!(*c.borrow(), 0); // 无 onEmpty hook
}
#[test]
fn worker_pool_bounded_and_close() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    let pool = WorkerPool::new(2);
    let counter = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(AtomicBool::new(false));
    // 两个任务阻塞在 gate 上，running 恒为 2，第三个必被拒绝（确定性）。
    let mk = |gate: Arc<AtomicBool>, counter: Arc<AtomicUsize>| {
        move || {
            while !gate.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            counter.fetch_add(1, Ordering::SeqCst);
        }
    };
    let t1 = pool.submit(mk(gate.clone(), counter.clone())).unwrap();
    let t2 = pool.submit(mk(gate.clone(), counter.clone())).unwrap();
    assert!(
        pool.submit(|| {}).is_err(),
        "third submit exceeds concurrency bound"
    );
    gate.store(true, Ordering::Release);
    for _ in 0..10000 {
        if t1.is_done() && t2.is_done() {
            break;
        }
        std::thread::yield_now();
    }
    assert!(t1.is_done() && t2.is_done());
    assert_eq!(counter.load(Ordering::SeqCst), 2);
    pool.close();
    assert!(pool.is_closed());
    assert!(pool.submit(|| {}).is_err());
}
#[test]
fn wake_one_shot_flag() {
    let w = Wake::new();
    assert!(!w.consume());
    w.fire();
    assert!(w.consume());
    assert!(!w.consume());
}
#[cfg(not(feature = "std"))]
#[test]
fn worker_pool_no_std_sync() {
    let mut pool = WorkerPool::new(1);
    let mut n = 0;
    pool.submit(|| n += 1).unwrap();
    assert_eq!(n, 1);
    pool.close();
    assert!(pool.submit(|| {}).is_err());
}
#[test]
fn drive_lifecycle_and_tick() {
    use std::rc::Rc;
    let mut drive = Drive::new(
        Runtime::new(
            Bus::new(),
            Rc::new(ManualClock::new(0)),
            Rc::new(SeqIdGen::new()),
        )
        .unwrap(),
    );
    assert!(drive.running());
    assert!(!drive.is_closed());
    let s = drive.tick().unwrap();
    assert!(s.is_quiescent);
    drive.kick();
    assert!(drive.wake().consume());
    drive.close();
    assert!(drive.is_closed());
    assert!(!drive.running());
    // closed 后 tick 返回 schedule 但不推进
    let _ = drive.tick().unwrap();
}
