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
    const STALE: &[&str] = &[
        "resolve_or_drop",
        "complete_parked",
        "finish_orphan",
        "fn park(",
        "pub fn publish(&mut self, topic",
        "fn reject_idle",
    ];
    let src = crate_dir().join("src");
    for (rel, text) in walk_rs(&src) {
        for token in STALE {
            assert!(!text.contains(token), "{rel} still has {token}");
        }
    }
    let dispatch = read("src/jobhost/dispatch.rs");
    assert!(dispatch.contains("resolve_only"));
    assert!(dispatch.contains("finish_safe"));
}

fn walk_rs(dir: &std::path::Path) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk_rs(&path));
        } else if path.extension().is_some_and(|x| x == "rs") {
            out.push((
                path.to_string_lossy().into_owned(),
                std::fs::read_to_string(&path).unwrap(),
            ));
        }
    }
    out
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
        "Job 两句",
        "延迟应答",
        "Matchmaker",
        "本轮不做",
    ] {
        assert!(readme.contains(p), "README missing {p}");
    }
    assert!(!readme.contains("Backpressure / State"));
}
