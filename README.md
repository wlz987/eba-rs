# eba-rs

进程内、单写者、合作式的 EBA 运行时（Rust）。单一 crate 用 feature 切换 std / no_std。

- `std`（默认）：宿主版，`UuidIdGen` / `MonotonicClock` / 线程池。
- `--no-default-features`：`no_std + alloc`，仅 `core::alloc`，可在裸机 target 编译（已验证 `thumbv7em-none-eabi`）。

## 快速开始

```rust
use eba_rs::*;
use std::rc::Rc;

struct HelloJob;
impl JobHooks for HelloJob {
    fn begin(&self, ctx: &mut Context) -> Vec<Effect> {
        ctx.finish(ok_payload(Value::Bool(true)));
        Vec::new()
    }
}

let id_gen: Rc<dyn IdGen> = Rc::new(SeqIdGen::new());
let clock: Rc<dyn Clock> = Rc::new(ManualClock::new(0));
let mut rt = Runtime::new(Bus::new(), clock, id_gen).unwrap();
rt.add(JobHost {
    actor_id: "hello".into(),
    inbox: Inbox::new(64),
    patterns: vec![parse_pattern("greet").unwrap()],
    make_job: Box::new(|_| Box::new(HelloJob)),
}).unwrap();

let pending = rt.ask("greet", Value::Null, "caller", None);
for _ in 0..20 {
    rt.advance().unwrap();
    if pending.is_ready() { break; }
}
let completion = pending.ready().unwrap(); // 结算后才兑现
assert!(is_result_ok(result_value(completion.body.as_ref().unwrap()).unwrap()));
```

`pending.ready()` 才算成功；手动推进时 `ManualClock` 不装真实定时器，时间只在 `clock.advance(ms)` 时变化。

## 设计要点

- **单写者**：`Runtime::advance(&mut self)` 是唯一推进入口，编译期借用检查排除重入。
- **Effect 联合**：11 支（ask/reply/hold/finish/settle/revoke/tell/record/barrier/timer/close），`match` 天然穷尽。
- **四相 pump**：READ（收信/resume）→ TIMER（到期唤醒）→ WORK（adopt/hook）→ DELIVERY（flush）。有限预算 + 连续 4 次无进展退出。
- **续跑读 live clock**：跨 timer / hold 的续跑读宿主 `Clock`，而非 hook 创建时的 pinned 值——否则 t=500 恢复时会拿 t=0 算 deadline 立即过期。
- **背压**：Inbox 有界 FIFO、queue/active/mailbox 限额；出站满则 `park → rearm → bury`（信封身份按 id，非对象引用），二次失败结算为 delivery_failed，不丢信。
- **Effect 落地约束**：`hold` 必须领先、`barrier/timer` 切分 checkpoint、checkpoint 前不得有终止 effect、settle/revoke 不得覆盖已 reply 的答案——非法组合抛错而非静默生效。
- **checkpoint 续跑**：barrier/timer 之后的 post effect 段延迟重放，读 live clock、透传外层 mint 语义。
- **回信路由**：每个 host 有独立 `reply.<actor>.**` 回信主题，嵌套 ask 的回信回到真正的发问 host。
- **结果即数据**：业务失败编码进 `Completion.body`（`err_payload`），不抛异常。
- **错误层级**：12 个具名错误构造器映射到 `EbaError` 的 12 个变体（`Bus`/`MailboxFull`/`InvalidTopic`/`AmbiguousRoute`/`Backpressure`/`Delivery`/`MailboxBusy`/`LeasePending`/`Wake`/`Clock`/`RecordSink`/`SeqExhausted`），并带 `is_*` 判定；池错误为 `PoolError` 的三个变体。
- **可调上限**：`Runtime::add_with_options(host, HostOptions)` 配置 queue/active/workBudget/mailbox/默认超时。

## 调度：由宿主驱动

没有内置事件循环或 executor：`Drive::tick()` 由宿主调用，返回 `Schedule` 告知下次唤醒时间；到期的 timer/租约进入待办队列，由 `advance()` 处理。调度权完全交给宿主，使运行时可在 no_std（无线程、无 executor）下工作。

## 设计取舍

以下均为语言/no_std 的必然取舍或明示的设计选择，语义自洽：

- **单写者守卫**：用 `&mut self` 编译期保证，而非运行时标志。
- **Effect 穷尽**：用 `enum` `match`，编译期穷尽检查。
- **payload 表示**：用 `Value` 枚举，确定性可比较、no_std 友好。
- **错误传播**：非法 Effect 组合使用 `panic!`（release profile `panic = "abort"`，直接终止）。
- **同步句柄**：`Pending::ready()`/`peek()` 轮询 + `Drive::tick()`，不用 Promise。
- **身份与引用**：用稳定的 u64 id 做 Agenda 键，pattern 匹配不切临时 Vec，无 schema 迁移用 `Cow::Borrowed`。
- **ID 格式**：`SeqIdGen` 输出 `e{n}`，`UuidIdGen` 带横线；ID 对外是不透明串，语义无关。
- **publish_latest**：`Bus::publish_latest` 合并同 topic 事件，属扩展 API。
- **Record 形状**：展平为 `{topic,payload,sv,seq,at,dedup_key}`，journal 语义由 sink 接口承载。
- **WorkerPool**：no_std 下退化为同步执行（concurrency=1）。
- `cancelPending` 恒空：同步取消不会失败，无需重试队列。
- `Bus::deliver`：满时暂存并由 `advance()` 重试（`flush_pending_deliveries`），无路由即丢弃，背压仍由 mailbox park→rearm→bury 承担。
- `close()` 返回 `Vec<Envelope>`，不带错误聚合。
- `Deadlines::add` 幂等（重复 id 静默返回）：lease 重提交需要此语义；Agenda 已在 update 时先 remove 再 add 保证刷新。
- `Watchers::notify` 无错误捕获：`Interest::notify` 是 `Fn()`，无 Result 可聚合。
- topic 校验强度与 trust 错误类型（`InvalidTopic`）：防御性更严格，错误统一到 `EbaError`。

## 当前状态

174 个测试全绿，其中 `tests/alignment.rs` 的 76 项回归锁定语义不变量。`cargo clippy --all-targets` 零告警，`fmt` / `no_std check` 通过。

## 命令

```sh
cargo test --features std                              # 174 个测试（含 76 项不变量回归）
cargo run --example quickstart --features std         # 快速开始
cargo check --no-default-features                     # no_std host
cargo check --no-default-features --target thumbv7em-none-eabi  # 裸机
cargo fmt && cargo clippy --features std              # 格式 + lint
cargo cov                                             # 覆盖率门禁（行覆盖 ≥ 85%，实测 86.09%）
```

测试分组：`contract`（主链路/背压/续跑/API）、`primitives`（信封/时钟/堆）、`notify`（Signal/Agenda/Pool）、`lease`（Lease/LeaseRegistry）、`records`（Schema/Record/Dedup/Rendezvous）、`bus`（路由/trust/claim）、`api`（错误/Effect/Context/Mailbox 边角）、`edges`（notify/pattern/hook 边角）、`alignment`（76 项不变量回归）。

## 边界与约定

- **进程内、内存态**：无持久化、恢复或跨进程投递；`MemorySink` 仅用于测试与示例。
- **host 互信**：`from` 由发送者自填；trust policy 只做主题级准入，不是沙箱，不做租户隔离。
- **入参由调用方保证**：topic 格式、payload 可线上传播、hook 同步返回、时间为安全非负整数。运行时只守自身状态机不变量。
- **payload 表示**：用 `Value` 枚举（Null/Bool/Int/Uint/Float/Str/Bytes/Seq/Map），确定性可比较，no_std 友好。
