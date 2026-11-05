# eba-rs

`eba-rs-infra` 的 **std 门面**：`pub use eba_rs_infra::*`，并默认启用 `std` / `uuid` / `drive`（tokio `Drive`）。

## 定位

- **不发布**：`publish = false`。它是纯 re-export，无独立代码，也不进 `cargo package` / `npm pack` 管线。
- **外部消费者请直接用 `eba-rs-infra`**（`features = ["std", "uuid", "drive"]`），它就是同一个 API。
- **本地开发**：保留在 workspace 里，方便 IDE / `cargo run` 直接面向"已含 std+Drive"的形态。
