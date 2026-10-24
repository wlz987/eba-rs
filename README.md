# eba-rs

进程内、单写者、合作式的 EBA 运行时的 Rust 实现，与 `eba-py` / `eba-ts` 语义对齐。
工作区里是两个平级、互不依赖的 crate：

| crate | 目标环境 | 特点 |
| --- | --- | --- |
| `eba-rs` | `std` + tokio | 带 `Drive` 异步泵；`uuid` 可选 |
| `eba-rs-infra` | `no_std` + `alloc`（默认开 `std`） | 无 `Drive` / `Caller`；`BTreeMap` / `BTreeSet`；由调用方手动推进 |

`eba-rs-infra` 默认特性是 `std` + `uuid`；`--no-default-features` 才是纯 `no_std` 那一体。

一次 request 交给每一台受理它的 Host 执行；调用方认领最先到的那份合法结果。副作用是否幂等，
由应用负责；超时只结清本地这一次等待。回调在返回前同步结束；`Payload` 在各 Job 之间共享、只读。

## 骨架

`Runtime` 按轮次推进：`add` / `advance` / `close` 各占一次单写者窗口，一轮只读一次钟。每台
`JobHost` 有四相（receive / expire / adopt / deliver）与按台预算。`Drive` 把 `advance` 当
异步循环跑，靠唤醒与期限醒来，不轮询。`Bus` 路由信封，`Inbox` 暂存信件，`Records` 记等待、
回复路由与租约，`Outbox` 按主题组押回信。

## 承诺

- 请求三条出口：合法结果、期限到点（`Completion::is_timeout`）、取消。
- 没设期限又没人接住的 request，期限就地记为已过（`time::deadlines::ELAPSED`），下一步交回
  空结局。设了期限的请求仍按期限走。
- 回信同主题一组、按进入顺序一封一封发；发布失败重试一次，第二次埋没，等 `close` 交还。
  收件箱收不下是背压（`MailboxFull`），不是投递故障。
- `close` 返回未认领的信；半途失败停在 `closing`，最后一次 `close` 收齐。
- 投递失败两次时，以 `delivery_failed` 结清它所回答的等待；钩子不答时补 `job_silent`。
- `std` 构建下，一次轮次里的 panic 会先复位 `busy` / 本轮观测值 / 读钟标志，再原样冒泡；
  运行时不会因为一次 panic 永久关在「轮次还开着」那一格。`no_std` 没有可捕获的 unwind，
  `busy` 只在正常返回时复位（读钟标志仍由 Drop 守卫复位）。

## Rust 侧的事实

- `Envelope` 是 `Arc<Envelope>`，入口即不可变；没有 py/ts 的「快照」这一步。payload 是
  `Arc<dyn Any + Send + Sync>`，取出靠 downcast。
- `timeout_ms` 是 `u64`，负数在类型层不可表达；`Receiver::try_recv` 交回
  `Result<Option<...>>`，不抛异常。
- 单写者由借用纪律承担：`Runtime` 全程持有 `&mut self`，`JobHost` 上没有被守卫的轮次动词。
- 违约即 panic，不是可 recover 的错误：非法的作业生命周期转移、同一只 `Wake` 挂第二个等待者、
  `Core::new` 收到非法 pattern。
- `Drive` 用 `Rc<RefCell<Runtime>>` 独占运行时；`start` 重复调用会 panic，`close` 等泵退出，
  `abort_pump` 只用于收尾/测试。泵跑在 `spawn_local` 上，调用方要有 `LocalSet`。
- `Call::pay` 用锁（`std` 是 `RwLock`，`infra` 是 `spin::RwLock`），因为 `Payload` 要求
  `Send + Sync`；这不是并发需要。

## 命令

```sh
cargo check -p eba-rs --no-default-features
cargo check -p eba-rs-infra --no-default-features
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo run --example order -p eba-rs
```

四端例子在根工作区由 `make examples` 统一跑；本仓 CI 跑静态闸与两套 `examples/*.rs`。

## 边界

- 按 ADR-0010，仓内没有测试；回归靠类型/写法闸门、例子与根工作区的端口一致性 audit。
- `no_std` 那一体在 panic 后不保证复位 `busy`；`Clock` 不会失败，也没有 `ClockError` 这条路。
- 两个 crate 的源码是刻意分开的近镜像（ADR-0015），不合并、不代码生成。
- 撤走测试之前的差异对名账见 [`HISTORY.md`](HISTORY.md)。
