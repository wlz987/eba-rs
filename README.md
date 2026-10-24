# eba-rs

Rust 端口：`eba-rs` 是 std + tokio，`eba-rs-infra` 是 `no_std` + `alloc`。

信任边界：Bus 上的 host 互信；`from_` 由发送者自填，不用于不可信插件或多租户隔离。

Rust std 捕获 pay/cancel hook 的 panic；`eba-rs-infra` 是 no_std，没有 unwind 边界。
