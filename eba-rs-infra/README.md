# eba-rs-infra

面向裸机 / 系统编程（如撰写操作系统）的 EBA 复刻项目，与 `eba-rs` 平级、互相独立。

## 定位

- `no_std` 核心，默认特性下亦可作为 `std` crate 编译。
- 与 `eba-rs` 模块同构：`envelope`、`pattern`、`time`、`notify`、`mail`、`rendezvous`、`job`、`runtime`、`errors`。
- 使用 `spin`（no_std 锁）与 `alloc` 集合，无 tokio / 线程 / 运行时依赖；由调用方通过 `Runtime::advance` 在裸机上手动驱动。
- 提供 `time::Clock`（含 `ManualClock`，`std` 特性下含 `MonotonicClock`）、`envelope::idgen::IdGen`（含 `SeqIdGen`，`uuid` 特性下含 `UuidIdGen`）。

## 校验

```bash
cargo test --workspace
cargo clippy --workspace --all-targets
cargo run -p eba-rs-infra --example order
cargo run -p eba-rs-infra --example bus
cargo build -p eba-rs-infra --no-default-features
```

`examples/` 与 `eba-py/examples/` 同形，只是这一侧没有 `Drive`：等待由 `Runtime::advance` 自己转圈，Caller 也要自己 `add` 上表。打印的句子仍然一样。

`eba-py/tests/test_drive.py` 那本因此在这一侧一格也问不出（不是借口，是账）：泵、闹钟、
等的人这三样零件都不存在——`wake.rs` 那张票据只等人叫醒，不会自己打盹；`Pending.ready`
这一只在 `tokio::sync::oneshot` 上的通道也没有，等的人由调用方自己 `advance` 到结清。
所以这十五格在这里只记账，不在测试里冒充问过：`unanswered_wait_is_told_that_the_drive_closed`、
`opening_a_ticket_on_a_finished_drive_is_refused`、`drive_that_died_tells_the_fault_once`、
`waiter_hears_the_fault_that_ended_the_pump`、`verdict_written_first_is_the_one_that_stands`、
`virtual_deadline_arms_no_wall_timer`、`wait_is_converted_from_milliseconds_once`、
`a_finished_drive_is_left_off_the_roster`、`a_late_exit_unregisters_only_itself`、
`a_second_round_does_not_carry_the_last_rounds_busy_flags`、`the_handles_are_handed_back_on_the_way_out`、
`a_stop_still_queued_when_the_door_shuts_is_carried_out`、
`releasing_three_cancels_buys_the_pump_no_extra_lap`（数的是「归还撤单的那一段不许多买一圈」：这一侧没有泵，转圈的是调用方自己，圈数不归谁多买，`_stops` 与 `_space_kicks` 两张表也都不存在）、
`registering_before_the_pump_ever_started_is_refused`、
`registering_on_a_pump_that_died_mid_round_is_refused`（这两格问的都是「登记撞在哪一张脸上」——`task` 还没装上，还是泵摔在半路而上下文没合上。这一侧 `open` 这一步整个不存在：登记就是调用方自己 `add` 上表再 `admit`，没有一张脸站在门口等着被问）。
末尾三格问的是 `_reset`、收工那一步交回的把手和还挂在名单上的撤单——这几样都不存在，第二轮就是 `Drive::new` 再走一遍；
名册只挂在泵上：这一侧没有站在台上的人，也就没有可擦的名字。
同一本在 `eba-rs/tests/drive.rs` 里逐格问着；与泵无关的那半句（「空位出现才叫醒」）在
`tests/wake.rs` 与 `tests/denials.rs` 问。

量法自己那一册在这侧也只记账：`test_the_snapshot_still_describes_this_tree`、`test_the_snapshot_records_a_sentence_for_every_alive_cell` 两格问的是 py 自己的 `measure/alive.json` 快照还说不说得出今天这棵树。这一侧没有快照可过期：`cargo test` 一格一名，行号搬家不带走任何一笔账。
## 特性

- `default = ["std", "uuid"]`
- `uuid` 隐式启用 `std`。
- `--no-default-features`：仅保留 `no_std` 核心。
