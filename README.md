# eba-rs

Rust 实现（std，单线程 `Rc` 句柄，poll 式泵）：单一库。三个原语承载全部并发——**Envelope** 封装消息（Header + 封闭 payload），**Bus** 订阅并投递，**Actor** 经唯一 Inbox 收信。**Job / JobHost / Clock** 只组合这三件，不是第四原语。

**逻辑并发单位是 Job**：一份 root 信封的完整编排生命周期。同一宿主上可多份 Job 并存；推进权在 `handle` / `poll`，不是一 Job 一线程。看门狗只扫 Active 的 inflight；`queue_limit` 只限尚未开工的队列。OS 线程调度不是本库原语。

**一条执行路线。** 会合只走 `start_request` → 四元组 `resolve_only` → `finish_safe`，认答唯一。收信只在 `JobHost.handle`：每步「分发 → 看门狗 → 冲刷开工队列」。结果信封立即认答、不进开工队列；等待发生在下一轮 handle 上，不宿在当前调用栈。发号只经借入的 `IdGenHandle`。

**两种装配，同一代数。** 封面：Actor 自 poll，可宽订主题。嵌入：宿主泵只调 `handle`，按显式主题列表窄订；Bus、Clock、IdGen 在调用期借入，不常驻持有。

**契约要点。**
- Bus 未命中静默；满箱整次回滚。只发不收用 Publisher。
- Job 两句：同步 `begin → reply → finish`；多步 `request`。外部等待由 Matchmaker 扣住请求信封再 `reply`，不引入新的第三态。
- 槽位：根按 cause（= id）、叶子按子请求 id，互不串扰；同一宿主可多份 Active。Clock 只扫 inflight。
- 软约束：Inbox 一读者、通配匹配宽化均不硬拒。
- 错误：统一 `Error`，违约与运营失败共用一条路线；`MaxInflight` 与队列满的背压均归于其中。

语言差不是第二套会合——`pub type Result` 与 `pub(crate)` 泵字段是语言习惯，不是第二条认答路线。封面可宽订、嵌入按主题列表窄订、泵在宿主只调 handle。本轮不做：取消传播、一流多结果、Header 加 reply-to、库级泵、每请求超时参数。

完整契约与原则见 [`PRINCIPLES.md`](PRINCIPLES.md)。

## 设计原则

设计简约正交普适，实现丰富完善；追求功能完备下的最小实现下界，兼求实现优化的上确界，偏好软约束而非硬约束。接口克制：克制导入导出、克制暴露、克制入参返回、克制重命名、克制大文件。规避死代码、规避冗余过时，规避内部冲突、维护内部的一致对齐；全局路线唯一，认答只经 resolve_only。延迟应答交 Matchmaker。

## 构建

```bash
cargo test
cargo run --example task_system
```
