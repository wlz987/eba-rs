# eba-rs-infra

EBA 的 `no_std` + `alloc` 核心，由调用方用 `Runtime::advance` 手动推进。
`std` feature 提供 std 时钟/错误；`drive` feature 提供 tokio Drive。
