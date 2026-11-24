#!/usr/bin/env bash
set -euo pipefail
# 覆盖率门禁：行覆盖 ≥ 85%（阈值按当前基线设定）。
cargo llvm-cov --features std --fail-under-lines 85 "$@"
