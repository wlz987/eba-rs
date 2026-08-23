# eba-rs

Rust 实现（std，单线程 `Rc` 句柄，poll 式泵）。三原语是 Envelope（承载）、Bus（投递）、Actor（收信）。Job / JobHost / Clock 只组合这三件，不是第四原语。

**逻辑并发单位是 Job**：一份 root 信封的完整编排生命周期。同一 `JobHost` 上可同时有多份 Active Job。推进权在 `handle` / `poll`，不是一 Job 一线程。看门狗只扫 Active 的 inflight。`queue_limit` 只限尚未开工队列。

```
src/
  envelope/   信封、topic、Value、MakeOptions
  idgen/      IdGen、SeqIdGen、UuidIdGen
  pattern/    topic 匹配
  inbox/      有界收件箱
  bus/        同步总线与订阅表
  subscriber/ 收信面
  publisher/  只发面
  registry/   会合台账
  result/     会合体代数与识别
  reply/      应答 / Matchmaker
  clock/      Clock、ManualClock、MonotonicClock
  job/        Job 两句：begin / request
  jobhost/    宿主泵：dispatch / slots / queue
tests/unit/
tests/integration/
examples/task_system/
```

- **Bus**：`subscribe` / `unsubscribe` / `publish`。未命中静默；满箱整次 `Error::MailboxFull` 回滚。只发不收用 `Publisher`（无 Inbox，不得订）。
- **收信**：`JobHost.handle`。每步 `dispatch` → 看门狗 → flush。会合结果立即认答。开泵时空箱则抽空本 Inbox；积压不插队。禁止重入。
- **会合**：只走 `Registry.start_request` → 四元组 `resolve_only` → `finish_safe`。发号只经借入的 `IdGenHandle`。
- **Job 两句**：同步 `begin → reply → finish`；多步 `request`。外部等待：请求方 `request`，应答方 `Matchmaker` 扣住信封再 `reply`。Clock 只扫 inflight。
- **槽位**：根按 cause（= id）；叶子按子请求 id。同一 Host 可多份 Active。
- **错误**：统一 `Error`（EnvelopeBuild / InvalidTopic / MailboxFull / QueueFull / MaxInflight / State），违约与运营失败共用一条路线。

原则见本库 [`PRINCIPLES.md`](PRINCIPLES.md)。

## 设计原则

| 原则 | 落地 |
|---|---|
| 设计简约普适 | 三原语 Envelope / Bus / Actor；Job 只组合 |
| 实现丰富完善 | 会合、看门狗、背压、延迟应答（Matchmaker）、单泵多 Job |
| 最小实现下界 | `start_request` → `resolve_only` → `finish_safe` |
| 软约束 | Inbox 一读者与 `**` 不硬拒；续抽以箱空为界 |
| 接口克制 | crate 根再导出编排入口；`pub(crate)` 泵细节 |
| 契约丰富 | [`PRINCIPLES.md`](PRINCIPLES.md) 与 README 同一套泵故事 |
| 克制导入导出 | 门面零 `as` 叠名 |
| 克制暴露 | `jobhost` 不进 crate 路径；槽位与借入字段非公开 |
| 克制大文件 | 按模块拆分 |
| 规避死代码 | 结果不进开工队列 |
| 规避内部冲突 | 会合不与开工队列混排 |
| 全局路线唯一 | 认答只经 `resolve_only` |

语言差不是第二套会合。`pub type Result` 与 `pub(crate)` 泵字段是语言习惯，不是第二条认答路线。封面可宽订；嵌入按 topic 列表窄订，泵在宿主只调 handle。本轮不做：取消传播、一流多结果、Header 加 reply-to、库级泵、每请求超时参数。

```bash
cargo test
cargo run --example task_system
```
