//! V6 门禁：publish_latest 的同 topic 合并语义（docs/os/20 §5 V6、05 IPC6）。
use eba_rs::*;
#[test]
fn publish_latest_coalesces_same_topic_events() {
    let bus = Bus::new();
    let inbox = Inbox::new(64);
    let receiver = inbox.receiver();
    bus.subscribe("sink", parse_pattern("event.tick").unwrap(), inbox);
    let ids = SeqIdGen::new();
    for i in 0..10 {
        let env = make_envelope(MakeEnvelope {
            topic: "event.tick".into(),
            payload: Value::Int(i),
            from: "src".to_string(),
            id_gen: &ids,
            address: Address::Event,
            cause: None,
            sv: 0,
        })
        .unwrap();
        assert!(bus.publish_latest(&env).unwrap());
    }
    assert!(receiver.borrow());
    let mut n = 0;
    let mut last = None;
    while let Some(env) = receiver.try_recv() {
        n += 1;
        last = Some(env.payload.clone());
    }
    receiver.unborrow();
    assert_eq!(n, 1, "V6: 10 次同 topic event 后队列应仅 1 条");
    assert_eq!(last, Some(Value::Int(9)), "V6: 保留的应是最新一条");
}
#[test]
fn publish_latest_keeps_distinct_topics() {
    let bus = Bus::new();
    let inbox = Inbox::new(64);
    let receiver = inbox.receiver();
    bus.subscribe("sink", parse_pattern("event.*").unwrap(), inbox);
    let ids = SeqIdGen::new();
    for topic in ["event.tick", "event.blk", "event.net"] {
        let env = make_envelope(MakeEnvelope {
            topic: topic.into(),
            payload: Value::Null,
            from: "src".to_string(),
            id_gen: &ids,
            address: Address::Event,
            cause: None,
            sv: 0,
        })
        .unwrap();
        assert!(bus.publish_latest(&env).unwrap());
    }
    assert!(receiver.borrow());
    let mut n = 0;
    while receiver.try_recv().is_some() {
        n += 1;
    }
    receiver.unborrow();
    assert_eq!(n, 3, "不同 topic 不应被合并");
}
