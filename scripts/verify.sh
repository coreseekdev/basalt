#!/usr/bin/env bash
# 形式化验证入口（docs/VERIFICATION.md §6 CI 分层）
# 用法: TLATOOLS=/path/tla2tools.jar VERUS=/path/verus ./scripts/verify.sh
set -euo pipefail
cd "$(dirname "$0")/.."
TLATOOLS="${TLATOOLS:-tla2tools.jar}"

echo "== TLA+ 数据面规约（账本 C1/C2/C4）=="
make -C spec check TLATOOLS="$TLATOOLS"

echo "== 阴性对照：EagerLeader 反例必须被检出（账本 C3，检查器判别力）=="
if make -C spec demo-eager TLATOOLS="$TLATOOLS" >/dev/null 2>&1; then
    echo "错误：注错场景通过——检查器判别力丢失，规约或不变式可能空洞" >&2
    exit 1
fi
echo "反例按预期检出"

echo "== Verus record 切片（账本 C5/C6/C6a）=="
"${VERUS:-verus}" --crate-type=lib verification/verus/record_core.rs

echo "== Kani L1 无 panic 门禁（账本 C13'）=="
if [ -n "${KANI:-}" ]; then
    PATH="$HOME/.kani/kani-0.67.0/bin:$PATH" kani verification/kani/record_l1.rs
    PATH="$HOME/.kani/kani-0.67.0/bin:$PATH" kani verification/kani/batch_header.rs
else
    echo "跳过（KANI 未设置）"
fi

echo "== record 回归（含 C13 crafted 报文）=="
cargo test -p basalt-record

echo "全部验证通过"
