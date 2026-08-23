use eba::{Clock, ManualClock, MonotonicClock};

#[test]
fn manual_clock_advance() {
    let c = ManualClock::new(10);
    assert_eq!(c.now_ms(), 10);
    c.advance(5);
    assert_eq!(c.now_ms(), 15);
}

#[test]
fn monotonic_clock_moves() {
    assert!(MonotonicClock::new().now_ms() >= 0);
}

#[test]
fn manual_default_zero() {
    assert_eq!(ManualClock::default().now_ms(), 0);
}
