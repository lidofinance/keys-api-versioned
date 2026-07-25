#!/usr/bin/env bash
# Benchmark one containerised service and report its peak memory.
#
# Load is generated on the host on purpose: a limited container would measure
# the generator. Memory comes from `docker stats`, so it is the number the
# cgroup limit applies to — not a host RSS reading.
#
# Usage: scripts/docker-bench.sh <container> <url> <label> [requests] [concurrency]
set -euo pipefail

container="${1:?container name}"
url="${2:?url}"
label="${3:?label}"
requests="${4:-5}"
concurrency="${5:-1}"

samples="$(mktemp)"
trap 'rm -f "$samples"' EXIT

# Sample the cgroup's memory usage while the load runs.
(
  while true; do
    docker stats --no-stream --format '{{.MemUsage}}' "$container" 2>/dev/null \
      | awk '{print $1}' >> "$samples" || true
  done
) &
sampler=$!
trap 'kill "$sampler" 2>/dev/null || true; rm -f "$samples"' EXIT

./bin/bench -label "$label" -url "$url" -requests "$requests" -concurrency "$concurrency" -warmup 1

kill "$sampler" 2>/dev/null || true
wait "$sampler" 2>/dev/null || true

# Normalise MiB/GiB to MiB and report the peak.
peak="$(awk '
  {
    v = $0
    sub(/[A-Za-z]+$/, "", v)
    unit = $0
    sub(/^[0-9.]+/, "", unit)
    if (unit == "GiB") v = v * 1024
    else if (unit == "KiB") v = v / 1024
    else if (unit == "B") v = v / 1048576
    if (v > max) max = v
  }
  END { printf "%.0f", max }
' "$samples")"

limit="$(docker inspect --format '{{.HostConfig.Memory}}' "$container" 2>/dev/null || echo 0)"
if [ "${limit:-0}" -gt 0 ]; then
  printf 'container mem  %s MiB peak of %s MiB limit\n' "$peak" "$((limit / 1048576))"
else
  printf 'container mem  %s MiB peak (no limit)\n' "$peak"
fi
