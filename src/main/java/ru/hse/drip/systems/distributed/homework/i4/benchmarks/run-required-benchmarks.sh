#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$SCRIPT_DIR"
while [[ ! -f "$ROOT_DIR/gradlew" && "$ROOT_DIR" != "/" ]]; do
  ROOT_DIR="$(cd "$ROOT_DIR/.." && pwd)"
done

if [[ ! -f "$ROOT_DIR/gradlew" ]]; then
  echo "Не удалось найти корень проекта с gradlew" >&2
  exit 1
fi

cd "$ROOT_DIR"

CLASSPATH="$(./gradlew -q printRuntimeClasspath --no-daemon)"
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done
}

trap cleanup EXIT

for port in 9101 9102 9103 9104 9105 9106 9107; do
  role="follower"
  if [[ "$port" == "9101" ]]; then
    role="leader"
  fi
  java -cp "$CLASSPATH" ru.hse.drip.systems.distributed.homework.i4.Main 127.0.0.1 "$port" "$role" \
    >"/tmp/i4-benchmark-node-$port.log" 2>&1 &
  PIDS+=("$!")
done

sleep 2

java -cp "$CLASSPATH" ru.hse.drip.systems.distributed.homework.i4.benchmarks.BenchmarkMain \
  --nodes "127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103,127.0.0.1:9104,127.0.0.1:9105,127.0.0.1:9106,127.0.0.1:9107" \
  --output "src/main/java/ru/hse/drip/systems/distributed/homework/i4/benchmarks/results.csv" \
  --report "src/main/java/ru/hse/drip/systems/distributed/homework/i4/report.md"
