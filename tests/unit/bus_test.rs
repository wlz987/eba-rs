use crate::util::env_n;
use eba::{Bus, Inbox};

#[test]
fn unmatched_publish_silent() {
    Bus::new().publish(&env_n("echo.gone", 1)).unwrap();
}

#[test]
fn fanout_and_full_rollback() {
    let bus = Bus::new();
    let (a, b) = (Inbox::new(1), Inbox::new(1));
    bus.subscribe("echo.**", &a).unwrap();
    bus.subscribe("echo.**", &b).unwrap();
    bus.publish(&env_n("echo.aa", 1)).unwrap();
    assert!(bus.publish(&env_n("echo.bb", 2)).is_err());
    assert!(a.try_recv().is_some());
    assert!(b.try_recv().is_some());
    assert!(a.try_recv().is_none());
}

#[test]
fn unsubscribe_stops_delivery() {
    let bus = Bus::new();
    let box_ = Inbox::new(4);
    bus.subscribe("echo.**", &box_).unwrap();
    assert!(bus.unsubscribe("echo.**", &box_).unwrap());
    bus.publish(&env_n("echo.aa", 1)).unwrap();
    assert!(box_.try_recv().is_none());
}

#[test]
fn try_recv_empty() {
    assert!(Inbox::new(1).try_recv().is_none());
}

#[test]
fn closed_inbox_skipped() {
    let bus = Bus::new();
    let box_ = Inbox::new(2);
    bus.subscribe("echo.**", &box_).unwrap();
    box_.close();
    bus.publish(&env_n("echo.aa", 1)).unwrap();
    assert!(box_.try_recv().is_none());
}

#[test]
fn unsubscribe_unknown_is_false() {
    let bus = Bus::new();
    assert!(!bus.unsubscribe("echo.**", &Inbox::new(1)).unwrap());
}

#[test]
fn double_subscribe_one_delivery() {
    let bus = Bus::new();
    let box_ = Inbox::new(4);
    bus.subscribe("echo.**", &box_).unwrap();
    bus.subscribe("echo.**", &box_).unwrap();
    bus.publish(&env_n("echo.aa", 1)).unwrap();
    assert!(box_.try_recv().is_some());
    assert!(box_.try_recv().is_none());
}

#[test]
fn same_inbox_two_patterns_one_copy() {
    let bus = Bus::new();
    let box_ = Inbox::new(4);
    bus.subscribe("echo.**", &box_).unwrap();
    bus.subscribe("**", &box_).unwrap();
    bus.publish(&env_n("echo.aa", 1)).unwrap();
    assert!(box_.try_recv().is_some());
    assert!(box_.try_recv().is_none());
}

#[test]
fn one_full_rolls_back_all() {
    let bus = Bus::new();
    let (room, tight) = (Inbox::new(2), Inbox::new(1));
    bus.subscribe("echo.**", &room).unwrap();
    bus.subscribe("echo.**", &tight).unwrap();
    bus.publish(&env_n("echo.aa", 1)).unwrap();
    assert!(bus.publish(&env_n("echo.bb", 2)).is_err());
    assert!(room.try_recv().is_some());
    assert!(room.try_recv().is_none());
    assert!(tight.try_recv().is_some());
}
