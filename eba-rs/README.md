# eba-rs

进程内、单写者、合作式的 EBA 运行时的 `std` + tokio 端口，与 `eba-py` / `eba-ts` 语义对齐，
与 `eba-rs-infra` 平级、互不依赖。

## 定位

- `std` 实现，承载 EBA 核心语义，并提供基于 tokio 的 `Drive` 异步驱动器。
- 模块与 `eba-rs-infra` 同构：`envelope`、`pattern`、`time`、`notify`、`mail`、`rendezvous`、
  `job`、`runtime`、`drive`、`errors`。
- 提供 `time::Clock`（含 `ManualClock`、`MonotonicClock`）、
  `envelope::idgen::IdGen`（含 `SeqIdGen`、`UuidIdGen`）。
- 单写者轮次是 `Runtime::{add, advance, close}`；`std` 构建下，轮次里的 panic 会先复位
  `busy` / 本轮观测值 / 读钟标志，再原样冒泡。

## 承诺

- 请求三条出口：合法结果、期限到点（`Completion::is_timeout`）、取消。
- 没设期限又没人接住的 request，期限就地记为已过（`ELAPSED`），交回空结局。
- 回信同主题一组、按进入顺序发；发布失败重试一次，第二次埋没，等 `close` 交还。
  收件箱收不下是背压（`MailboxFull`），不是投递故障。
- `close` 返回未认领的信；投递失败两次以 `delivery_failed` 结清等待；钩子不答补 `job_silent`。
- 错误结果可带域自定义整数 `code`（`err_payload_code` / `result_code`）。
- 服务端可在 `ctx.hold()` 扣住一个 request，稍后用 `Reply::send/fail` 写槽；`advance` 末尾结算。

## 例子

```sh
cargo run --example order
cargo run --example bus
```

四端例子与端口一致性 audit 在仓库根工作区跑。按 ADR-0010，仓内没有测试；回归靠静态闸与例子。
