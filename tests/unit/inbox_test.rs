use crate::util::env_n;
use eba::{ActorId, Inbox, Subscriber};

#[test]
#[should_panic(expected = "capacity must be >= 1")]
fn capacity_rejects_zero() {
    Inbox::new(0);
}

#[test]
fn close_clears_and_stays_empty() {
    let box_ = Inbox::new(2);
    assert!(box_.try_enqueue(env_n("echo.x", 1)));
    box_.close();
    assert!(box_.is_closed());
    assert!(box_.is_empty());
    assert!(box_.try_recv().is_none());
    assert!(!box_.try_enqueue(env_n("echo.x", 2)));
    box_.close();
    assert!(box_.is_closed());
}

#[test]
fn enqueue_full_then_recv() {
    let box_ = Inbox::new(1);
    assert!(box_.try_enqueue(env_n("echo.x", 1)));
    assert!(!box_.try_enqueue(env_n("echo.x", 2)));
    assert!(box_.try_recv().is_some());
    assert!(box_.try_recv().is_none());
}

#[test]
fn drop_last_identity() {
    let box_ = Inbox::new(2);
    let (a, b) = (env_n("echo.x", 1), env_n("echo.x", 2));
    assert!(box_.try_enqueue(a.clone()));
    assert!(!box_.try_drop_last(&b));
    assert!(box_.try_drop_last(&a));
    assert!(box_.is_empty());
    assert!(!box_.try_drop_last(&a));
}

#[test]
fn note_reader_soft_second_actor() {
    let box_ = Inbox::new(2);
    Subscriber::new(ActorId("one".into()), &box_, &[]);
    Subscriber::new(ActorId("two".into()), &box_, &[]);
    assert!(box_.try_enqueue(env_n("echo.x", 1)));
}

#[test]
fn inbox_ids_are_unique() {
    let (a, b) = (Inbox::new(1), Inbox::new(1));
    assert_ne!(a.id(), b.id());
}
