#!/usr/bin/env bash
# T-M3.5 docker-compose 一键 e2e：compose 拉起 basalt broker 容器 →
# 依次跑 4 个 rdkafka 模板（对 localhost:9092）→ 收尾 down -v。
# docker compose v2 优先，v1 docker-compose 兼容回退。
set -u
cd "$(dirname "$0")/../.."

DC=""
if docker compose version >/dev/null 2>&1; then
  DC="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  DC="docker-compose"
else
  echo "FAIL: docker compose / docker-compose 均不可用" >&2
  exit 2
fi

if command -v ss >/dev/null 2>&1 && ss -tln 2>/dev/null | grep -q ":9092 "; then
  echo "FAIL: 宿主 9092 已被占用（compose 端口映射会冲突），请先释放" >&2
  exit 2
fi

$DC down --remove-orphans --volumes >/dev/null 2>&1 || true

echo "[0] $DC up -d --build（首次构建含 cargo release，需数分钟）"
if ! $DC up -d --build; then
  echo "FAIL: compose up" >&2
  exit 2
fi
cleanup() { $DC down --remove-orphans --volumes >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "[0] 等待 broker 端口 9092 …"
ok=""
for _ in $(seq 1 60); do
  if timeout 1 bash -c "</dev/tcp/127.0.0.1/9092" 2>/dev/null; then ok=1; break; fi
  sleep 1
done
if [ -z "$ok" ]; then
  echo "FAIL: broker 端口 60s 未就绪，容器日志尾：" >&2
  $DC logs --tail 40 basalt >&2 || true
  exit 2
fi
echo "[0] broker 就绪"

SCRIPTS=(librdkafka_assign.py vector_drain_commit.py bento_checkpoint_limit.py librdkafka_cooperative.py)
FAILS=0
for s in "${SCRIPTS[@]}"; do
  echo "=== [run] $s ==="
  if python3 "testing/e2e/$s"; then
    continue
  fi
  FAILS=$((FAILS + 1))
  echo "--- $s FAIL（compose broker 日志尾） ---"
  $DC logs --tail 60 basalt 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | grep -E "WARN|ERROR|panic" | tail -8
done

if [ $FAILS -eq 0 ]; then
  echo "PASS ✔ (docker-compose 一键 e2e: ${#SCRIPTS[@]}/${#SCRIPTS[@]})"
else
  echo "FAIL: ${FAILS}/${#SCRIPTS[@]} 红"
fi
exit $FAILS
