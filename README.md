# eba-rs

Rust 端口：`eba-rs` 是 `eba-rs-infra` 的 std 门面（core + tokio Drive）；`eba-rs-infra` 是 `no_std` + `alloc` 核心。

信任边界：Bus 上的 host 互信；`from_` 由发送者自填，不用于不可信插件或多租户隔离。

std 下 pay panic 先记录再 resume（调用方可见），cancel hook panic 记录后继续；no_std 下 panic 直接 abort。两种行为由 `std` feature 选择。
