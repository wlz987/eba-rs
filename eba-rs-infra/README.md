# eba-rs-infra

面向裸机 / 系统编程的 EBA 端口：`no_std` + `alloc`，与 `eba-rs` 平级、互不依赖。

## 定位

- 默认特性 `std` + `uuid`；`--no-default-features` 才是纯 `no_std` 那一体。
- 没有 `Drive` 与 `Caller`：由调用方用 `Runtime::advance` 手动驱动，Caller 自己 `add` 上表。
- 模块与 `eba-rs` 同构；用 `BTreeMap` / `BTreeSet` 求确定性顺序，`Call::pay` 用
  `spin::RwLock`（`Payload` 要求 `Send + Sync`），crate 根 `#![forbid(unsafe_code)]`。
- 提供 `time::Clock`（`std` 特性下含 `MonotonicClock`）、`envelope::idgen::IdGen`
  （`uuid` 特性下含 `UuidIdGen`）。

## 边界

- `std` 构建下，轮次里的 panic 会先复位 `busy` / 观测值 / 读钟标志再冒泡；`no_std` 没有可捕获
  的 unwind，`busy` 只在正常返回时复位（读钟标志仍由 Drop 守卫复位）。
- `Clock` 不会失败，没有 `ClockError`；非法生命周期转移、同一只 `Wake` 挂第二个等待者等
  违约行为 panic。
- 错误结果可带域自定义整数 `code`（`err_payload_code` / `result_code`）。
- 按 ADR-0010，仓内没有测试；回归靠静态闸与例子。

## 例子

```sh
cargo run --example order -p eba-rs-infra
cargo run --example bus -p eba-rs-infra
cargo build -p eba-rs-infra --no-default-features
```
