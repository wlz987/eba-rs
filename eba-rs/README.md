# eba-rs

面向一般开发用户的 EBA 复刻项目，与 `eba-rs-infra` 平级、互相独立。

## 定位

- `std` 实现，承载 EBA 核心语义，并提供基于 tokio 的 `Drive` 异步驱动器。
- 模块与 `eba-rs-infra` 同构：`envelope`、`pattern`、`time`、`notify`、`mail`、`rendezvous`、`job`、`runtime`、`drive`、`errors`。
- 提供 `time::Clock`（含 `ManualClock`、`MonotonicClock`）、`envelope::idgen::IdGen`（含 `SeqIdGen`、`UuidIdGen`）。

## 校验

```bash
cargo test --workspace
cargo clippy --workspace --all-targets
cargo run -p eba-rs --example order
cargo run -p eba-rs --example bus
```

`examples/` 是 `eba-py/examples/` 的同形件：同样两台 Host、同一次下单、同一只满箱，正文也带同一个形状
（下单与结局都挂着 `{"sku": ...}` 一个映射——py 与 ts 如此，这边 rs 用 `HashMap`、infra 用 `BTreeMap`）。
打印出来的是同一批事实，句子按各自语言的写法：`Payload` 是 `Arc<dyn Any>`，没有通用可打印的形状。
这句「同一批事实」不只靠眼睛对：`python3 .scratch/eba-port-transcription/tools/diff-examples.py`
把 py / rs / infra / ts 四台机器跑出来的事实剥出来逐条比。
它们由 `cargo test --workspace` 一起编译，所以门面 API 一旦少了一格，例子先报错。
