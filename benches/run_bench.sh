#!/usr/bin/env bash
set -eu
DATA=$(mktemp -d /tmp/basalt-bench-XXXX)
cleanup() { kill -9 $PID 2>/dev/null; wait $PID 2>/dev/null; rm -rf "$DATA"; }
trap cleanup EXIT
BASALT_DATA_DIR="$DATA" BASALT_PORT=9092 BASALT_NUM_PARTITIONS=2 \
  ./target/release/basalt-server > /dev/null 2>&1 &
PID=$!
sleep 1
python3 benches/throughput.py
