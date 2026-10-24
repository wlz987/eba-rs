# eba-rs

Rust 端口：`eba-rs` 是 std + tokio，`eba-rs-infra` 是 `no_std` + `alloc`。

信任边界：Bus 上的 host 互信；`from_` 由发送者自填，不用于不可信插件或多租户隔离。

Rust std 的 pay panic 先记录再 resume（调用方可见），cancel hook panic 记录后继续；`eba-rs-infra` 是 no_std，panic 直接 abort。
