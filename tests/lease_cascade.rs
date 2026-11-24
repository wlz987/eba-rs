//! V9 门禁：三层并发的 Lease 嵌套与 correlationId 级联取消（docs/os/12 LYR2/3/5）。
use eba_rs::*;
#[test]
fn v9_nested_leases_and_cascade_cancel() {
    let mut reg = LeaseRegistry::new();
    // L1: 顶层 job 等待 worker；owner = "caller"
    reg.submit(
        "L1".into(),
        Lease::new(LeaseOptions::default()),
        "caller".into(),
        "top",
        None,
        "sys.reply",
        None,
    )
    .unwrap();
    // L2: L1 等待中的子请求；owner = "L1"（correlationId 沿原路）
    reg.submit(
        "L2".into(),
        Lease::new(LeaseOptions::default()),
        "L1".into(),
        "mid",
        None,
        "sys.reply",
        None,
    )
    .unwrap();
    // L3: L2 等待中的子请求；owner = "L2"
    reg.submit(
        "L3".into(),
        Lease::new(LeaseOptions::default()),
        "L2".into(),
        "leaf",
        None,
        "sys.reply",
        None,
    )
    .unwrap();
    assert_eq!(reg.size(), 3);
    // 取消顶层 owner → 三层全部失效（LYR5）。
    reg.cancel_cascade(&"caller".into());
    assert_eq!(reg.size(), 0, "all nested leases must be cancelled");
    let mut cancelled = reg.take_cancelled();
    cancelled.sort();
    assert_eq!(cancelled, vec!["L1", "L2", "L3"]);
}
#[test]
fn v9_correlation_id_routes_result_back_along_chain() {
    let mut reg = LeaseRegistry::new();
    reg.submit(
        "req".into(),
        Lease::new(LeaseOptions::default()),
        "owner1".into(),
        "stage1",
        None,
        "reply.owner1",
        Some("req".into()),
    )
    .unwrap();
    // 构造回信信封：Address::Result{correlation_id: "req"}
    let ids = SeqIdGen::new();
    let env = make_envelope(MakeEnvelope {
        topic: "reply.owner1".into(),
        payload: Value::Int(7),
        from: "worker".into(),
        id_gen: &ids,
        address: Address::Result {
            correlation_id: "req".into(),
        },
        cause: Some("req".into()),
        sv: 0,
    })
    .unwrap();
    let (owner, stage, completion) = reg
        .take_result(&env)
        .expect("result routed by correlation_id")
        .unwrap();
    assert_eq!(owner.as_str(), "owner1");
    assert_eq!(stage, "stage1");
    // take_result 把 result 信封的 payload 原样作为 completion.body。
    assert_eq!(completion.body.as_ref().unwrap(), &Value::Int(7));
}
