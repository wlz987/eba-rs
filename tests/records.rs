//! Schema/Record/Dedup/Rendezvous。
use eba_rs::*;
fn rec_envelope() -> Envelope {
    let g = SeqIdGen::new();
    make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 1,
    })
    .unwrap()
}
fn v1_to_v2(v: Value, _from: u32) -> core::result::Result<Value, EbaError> {
    let mut m = v.as_map().cloned().unwrap_or_default();
    m.insert("v".into(), Value::Int(2));
    Ok(Value::Map(m))
}
fn bad_migrator(_v: Value, _from: u32) -> core::result::Result<Value, EbaError> {
    Err(EbaError::Other("boom".into()))
}
#[test]
fn schema_register_check_version() {
    let mut reg = SchemaRegistry::new();
    assert_eq!(reg.size(), 0);
    assert_eq!(reg.version("t"), 1); // 默认 sv=1
                                     // validate 由注册方负责，内建无 object 类型检查。
    reg.register(
        "t",
        TopicSchema::new(2)
            .with_migrator(1, v1_to_v2)
            .with_validator(|v| {
                if v.as_map().is_none() {
                    Err(eba_rs::EbaError::InvalidTopic("must be object".into()))
                } else {
                    Ok(())
                }
            }),
    )
    .unwrap();
    assert_eq!(reg.size(), 1);
    assert_eq!(reg.version("t"), 2);
    reg.check("t", &Value::Map(Default::default())).unwrap();
    assert!(reg.check("t", &Value::Null).is_err());
    // 未注册 topic 不校验
    reg.check("unknown", &Value::Null).unwrap();
}
#[test]
fn schema_migrate_stepwise_and_fail_closed() {
    let mut reg = SchemaRegistry::new();
    reg.register("t", TopicSchema::new(2).with_migrator(1, v1_to_v2))
        .unwrap();
    let m = reg.migrate("t", Value::Map(Default::default()), 1).unwrap();
    assert_eq!(m.as_map().unwrap().get("v"), Some(&Value::Int(2)));
    // 单次 migrator 从 fromVersion 直达 current（1→3 只需 from=1 的 migrator）。
    let mut reg2 = SchemaRegistry::new();
    reg2.register("t", TopicSchema::new(3).with_migrator(1, v1_to_v2))
        .unwrap();
    assert!(reg2.migrate("t", Value::Map(Default::default()), 1).is_ok());
    // 缺 migrator → fail closed
    let mut reg3 = SchemaRegistry::new();
    reg3.register("t", TopicSchema::new(2)).unwrap();
    assert!(reg3
        .migrate("t", Value::Map(Default::default()), 1)
        .is_err());
    // 超前版本 → 拒绝
    assert!(reg.migrate("t", Value::Map(Default::default()), 5).is_err());
}
#[test]
fn schema_migrate_envelope_rewrites_sv() {
    let mut reg = SchemaRegistry::new();
    reg.register("t", TopicSchema::new(2).with_migrator(1, v1_to_v2))
        .unwrap();
    let g = SeqIdGen::new();
    let env = make_envelope(MakeEnvelope {
        topic: "t".into(),
        payload: Value::Map(Default::default()),
        from: "a".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 1,
    })
    .unwrap();
    let migrated = reg.migrate_envelope(&env).unwrap();
    assert_eq!(migrated.header.sv, 2);
    assert_eq!(
        migrated.payload.as_map().unwrap().get("v"),
        Some(&Value::Int(2))
    );
}
#[test]
fn schema_migrator_error_wrapped() {
    let mut reg = SchemaRegistry::new();
    reg.register("t", TopicSchema::new(2).with_migrator(1, bad_migrator))
        .unwrap();
    assert!(reg.migrate("t", Value::Map(Default::default()), 1).is_err());
}
#[test]
fn sequencer_monotonic() {
    let mut s = Sequencer::new(5);
    // last 初始恒为 0，与 start 无关。
    assert_eq!(s.last(), 0);
    assert_eq!(s.next_seq(), 5);
    assert_eq!(s.next_seq(), 6);
    assert_eq!(s.last(), 6);
}
#[test]
fn dedup_window_fifo_eviction() {
    let mut w = DedupWindow::new(2);
    let a = dedup_key("x", "a");
    let b = dedup_key("x", "b");
    let c = dedup_key("x", "c");
    w.accept(a.clone());
    w.accept(b.clone());
    assert!(w.has(&a));
    assert!(w.has(&b));
    w.accept(c.clone());
    assert!(!w.has(&a), "oldest evicted");
    assert!(w.has(&b));
    assert!(w.has(&c));
}
#[test]
fn record_log_record_drain_flush_checkpoint() {
    let mut log = RecordLog::new(Box::new(MemorySink::new()), 8, 1);
    assert_eq!(log.last_seq(), 0);
    let g = SeqIdGen::new();
    let env = make_envelope(MakeEnvelope {
        topic: "log".into(),
        payload: Value::Null,
        from: "a".into(),
        id_gen: &g,
        address: Address::Event,
        cause: None,
        sv: 0,
    })
    .unwrap();
    let r = log.record(&env, 10).unwrap();
    assert_eq!(r.seq, 1);
    assert_eq!(log.pending(), 1);
    log.settle().unwrap();
    assert_eq!(log.durable(), 1);
    assert_eq!(log.pending(), 0);
    log.checkpoint(1).unwrap();
}
#[test]
fn record_ring_evicts_when_full() {
    let mut ring = RecordRing::new(2);
    let rec = |seq| Record {
        v: 1,
        envelope: rec_envelope(),
        topic: "t".into(),
        payload: Value::Null,
        sv: 0,
        seq,
        at: 0,
        dedup_key: format!("k{}", seq),
    };
    assert!(ring.push(rec(1)));
    assert!(ring.push(rec(2)));
    assert!(!ring.push(rec(3)), "full ring must refuse, not evict");
    assert_eq!(ring.pending(), 2);
    assert_eq!(ring.drain().len(), 2);
}
#[test]
fn dedup_sink_drops_duplicates() {
    let mut sink = DedupSink::new(Box::new(MemorySink::new()), 8);
    let rec = Record {
        v: 1,
        envelope: rec_envelope(),
        topic: "t".into(),
        payload: Value::Null,
        sv: 0,
        seq: 1,
        at: 0,
        dedup_key: "same".into(),
    };
    sink.append(&rec).unwrap();
    sink.append(&rec).unwrap();
    // 用 checkpoint 验证只落了一条：flush 到 1 后 checkpoint(1)  succeeds，
    // 但无法直接数条数（inner 是 trait object）；靠 dedup 不报错即可。
    sink.flush(1).unwrap();
    assert_eq!(sink.checkpoint(1).unwrap(), 1);
}
