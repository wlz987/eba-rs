use std::fs;
use std::path::PathBuf;

fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read(rel: &str) -> String {
    fs::read_to_string(crate_dir().join(rel)).unwrap()
}

#[test]
fn principles_match_workspace_agent_when_present() {
    let principles = read("PRINCIPLES.md");
    let agent = crate_dir().parent().unwrap().join("AGENT.md");
    if agent.exists() {
        assert_eq!(principles, fs::read_to_string(agent).unwrap());
    }
    for line in [
        "追求软约束",
        "克制导入",
        "克制暴露",
        "规避死代码",
        "全局路线唯一",
    ] {
        assert!(principles.contains(line), "missing {line}");
    }
}

#[test]
fn jobhost_module_is_private() {
    let lib = read("src/lib.rs");
    assert!(lib.contains("mod jobhost;"));
    assert!(!lib.contains("pub mod jobhost;"));
    assert!(!lib.contains("EnvelopeQueue"));
    assert!(!lib.contains("has_result_shape"));
    assert!(!lib.contains("resolve_or_drop"));
}

#[test]
fn job_host_inbox_not_public_field() {
    let host = read("src/jobhost/mod.rs");
    let after = host.split("pub struct JobHost").nth(1).unwrap();
    let body = after.split("impl JobHost").next().unwrap();
    assert!(body.contains("pub(crate) inbox:"));
    assert!(body.contains("pub(crate) actor_id:"));
    assert!(!body.contains("pub inbox:"));
    assert!(!body.contains("pub actor_id:"));
}

#[test]
fn sources_single_rendezvous_route() {
    for rel in [
        "src/jobhost/dispatch.rs",
        "src/registry/mod.rs",
        "src/lib.rs",
    ] {
        let text = read(rel);
        assert!(!text.contains("resolve_or_drop"), "{rel}");
        if rel.ends_with("dispatch.rs") {
            assert!(text.contains("resolve_only"));
            assert!(text.contains("finish_orphan") || text.contains("finish_safe"));
        }
    }
}

#[test]
fn readme_agent_table_and_error_names() {
    let readme = read("README.md");
    for p in [
        "设计简约",
        "实现丰富",
        "最小实现下界",
        "软约束",
        "接口克制",
        "克制导入导出",
        "克制暴露",
        "克制大文件",
        "规避死代码",
        "规避内部冲突",
        "全局路线唯一",
        "resolve_only",
        "MaxInflight",
        "语言差不是第二套会合",
    ] {
        assert!(readme.contains(p), "README missing {p}");
    }
    assert!(!readme.contains("Backpressure / State"));
}
