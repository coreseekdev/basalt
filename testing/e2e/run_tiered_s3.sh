#!/usr/bin/env bash
# T-M4.3 v2：S3 适配器实机 e2e（端点 = 自研 durad，SigV4 兼容 S3 API）。
# 前置：durad 运行中（127.0.0.1:19000，信任模式），桶 basalt-tiered 存在。
#   [1] BASALT_OBJECT_STORE=s3：produce → 上传（对象落在 durad）
#   [2] 本地回收 + 读穿透全量
#   [3] broker 重启 → 注册表从 S3 重建 → 再读全量
#   [4] 对象面抽查：List 前缀命中、Get range、If-None-Match create CAS
set -u
PORT="${TIERED_S3_PORT:-9092}"  # tiered_storage.py 硬编码 9092
S3_EP="${BASALT_S3_ENDPOINT:-http://127.0.0.1:19000}"
BUCKET="${BASALT_S3_BUCKET:-basalt-tiered}"
DATA=$(mktemp -d /tmp/basalt-ts3-XXXX)
LOG=$(mktemp /tmp/basalt-ts3-log-XXXX)

OLD=$(ss -tlnp 2>/dev/null | grep ":$PORT " | grep -oP 'pid=\K[0-9]+' | head -1)
[ -n "$OLD" ] && kill -9 "$OLD" 2>/dev/null && sleep 0.5

start_server() {
  BASALT_DATA_DIR="$DATA" BASALT_PORT="$PORT" BASALT_NUM_PARTITIONS=2 BASALT_METRICS_PORT=0 \
    BASALT_STORAGE_MODE=tiered BASALT_SEGMENT_MAX_BYTES=2048 \
    BASALT_OBJECT_STORE=s3 BASALT_S3_ENDPOINT="$S3_EP" \
    BASALT_S3_BUCKET="$BUCKET" BASALT_S3_KEY=dummy BASALT_S3_SECRET=dummy \
    BASALT_LOG_LEVEL="${BASALT_LOG_LEVEL:-warn}" \
    ./target/debug/basalt-server > "$LOG" 2>&1 &
  PID=$!
}
stop_server() {
  kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null
}
trap 'stop_server; rm -rf "$DATA" "$LOG"' EXIT
start_server
for _ in $(seq 1 40); do
  (exec 3<>/dev/tcp/localhost/$PORT) 2>/dev/null && exec 3>&- && break
  sleep 0.25
done
sleep 0.5

python3 testing/e2e/tiered_storage.py produce || { echo "--- log ---"; tail -5 "$LOG"; exit 1; }
sleep 1

# [1] 对象落在 durad（S3 List 前缀 = 段对象 key 前缀）
OBJ_COUNT=$(curl -s -m 5 "$S3_EP/$BUCKET?list-type=2&prefix=ts-e2e" | grep -o "<Key>" | wc -l)
echo "s3 objects under ts-e2e: $OBJ_COUNT"
[ "$OBJ_COUNT" -ge 12 ] || { echo "FAIL: S3 对象过少（$OBJ_COUNT < 12）"; exit 1; }
echo "[1] objects uploaded to S3 endpoint ✔"

# [2] 本地回收 + 读穿透
LOCAL_LOGS=$(find "$DATA" -name "*.log" -not -path "*objectstore*" | wc -l)
echo "local segments: $LOCAL_LOGS"
[ "$LOCAL_LOGS" -le 6 ] || { echo "FAIL: 本地段未回收（$LOCAL_LOGS > 6）"; exit 1; }
python3 testing/e2e/tiered_storage.py consume || { tail -5 "$LOG"; exit 1; }
echo "[2] local recycled + read-through full ✔"

# [3] 重启恢复：注册表从 S3 重建
stop_server
start_server
sleep 1.5
python3 testing/e2e/tiered_storage.py consume || { tail -5 "$LOG"; exit 1; }
echo "[3] restart recovery from S3 registry ✔"

# [4] 原语抽查：If-None-Match create CAS（durad 语义 = 412/ETag 回读）
ETAG=$(curl -s -m 5 -X PUT "$S3_EP/$BUCKET/probe-cas" -H "If-None-Match: *" -D - -o /dev/null | grep -i "^etag:" | tr -d "\r" | cut -d" " -f2)
[ -n "$ETAG" ] || { echo "FAIL: create 未回 ETag"; exit 1; }
CODE=$(curl -s -m 5 -X PUT "$S3_EP/$BUCKET/probe-cas" -H "If-None-Match: *" -D - -o /dev/null | grep -E "^HTTP" | grep -oE "[0-9]{3}")
[ "$CODE" = "412" ] || { echo "FAIL: 重复 create 应 412，得 $CODE"; exit 1; }
# range 读
BODY=$(curl -s -m 5 "$S3_EP/$BUCKET/probe-cas" -H "Range: bytes=0-3")
[ -n "$BODY" ] || { echo "FAIL: range 读为空"; exit 1; }
# delete + 404
curl -s -m 5 -X DELETE "$S3_EP/$BUCKET/probe-cas" -o /dev/null
CODE=$(curl -s -m 5 -o /dev/null -w "%{http_code}" "$S3_EP/$BUCKET/probe-cas")
[ "$CODE" = "404" ] || { echo "FAIL: 删除后应 404，得 $CODE"; exit 1; }
echo "[4] create CAS (ETag/412) + range + delete ✔"

PASS_MSG="PASS ✔ (S3 实机：durad 上传/本地回收/读穿透/重启恢复/四原语)"
echo "$PASS_MSG"
