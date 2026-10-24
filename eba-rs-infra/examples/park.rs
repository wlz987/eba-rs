use std::sync::Arc;

use eba_rs_infra::envelope::result::ok_payload;
use eba_rs_infra::job::Reply;
use eba_rs_infra::park::Park;
use eba_rs_infra::Payload;

fn main() {
    let mut park: Park<&str, i64> = Park::new();

    let first = Reply::new();
    let second = Reply::new();
    assert!(park.wait("k", first, None, None).is_none());
    assert!(park.wait("k", second, None, None).is_none());
    assert!(park.wake(&"k", 7).is_some());
    assert!(park.wake(&"k", 8).is_some());
    assert!(park.wake(&"k", 9).is_none());

    let credit = Reply::new();
    assert_eq!(park.wait("k", credit.clone(), None, None), Some(9));
    credit
        .send(ok_payload(Arc::new(9i64) as Payload))
        .expect("credit settled");

    let left = Reply::new();
    let right = Reply::new();
    assert!(park.wait("move", left, None, None).is_none());
    assert!(park.wait("move", right, None, None).is_none());
    assert_eq!(park.requeue(&"move", "next", 1, 1).len(), 1);
    assert!(park.wake(&"next", 1).is_some());

    let late = Reply::new();
    assert!(park.wait("deadline", late, Some(5), None).is_none());
    assert!(park.expire(4).is_empty());
    assert_eq!(park.expire(5).len(), 1);

    assert!(park.wait("cancel", Reply::new(), None, None).is_none());
    assert!(park.wait("cancel", Reply::new(), None, None).is_none());
    assert_eq!(park.cancel(&"cancel").len(), 2);
    assert!(park.keys().is_empty());

    let mut owned: Park<&str, i64, u64> = Park::new();
    let owner_a = Reply::new();
    let owner_b = Reply::new();
    assert!(owned.wait("own", owner_a, None, Some(1)).is_none());
    assert!(owned.wait("own", owner_b, None, Some(2)).is_none());
    assert_eq!(owned.cancel_owner(&1).len(), 1);
    assert!(owned.wake(&"own", 1).is_some());
    assert!(owned.keys().is_empty());

    let mut capped: Park<&str, i64> = Park::new();
    for value in 0..4097i64 {
        capped.wake(&"cap", value);
    }
    for value in 0..4096i64 {
        assert_eq!(capped.wait("cap", Reply::new(), None, None), Some(value));
    }
    assert_eq!(capped.wait("cap", Reply::new(), None, None), None);

    println!("park ok");
}
