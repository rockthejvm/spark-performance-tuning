#!/usr/bin/env bash
# Step 5 — run this in a SECOND terminal WHILE a submit is in progress.
#
# It samples the live executor pod's CFS accounting from cgroup v2 (/sys/fs/cgroup/cpu.stat).
#   nr_throttled    -> how many scheduling periods the pod was throttled in
#   throttled_usec  -> total microseconds the pod spent throttled (waiting for quota)
# On the THROTTLED run both climb steadily. On the BURSTABLE run they stay ~flat. That is the
# kernel-level proof that backs up the wall-clock difference — and it is the ONLY signal you
# get, because Spark logs nothing about it.
set -euo pipefail
K3S=spark4-k3s
NS=spark-demo

echo ">> Watching executor cpu.stat every 3s (Ctrl-C to stop)..."
while true; do
  POD=$(docker exec "$K3S" kubectl -n "$NS" get pods -l spark-role=executor \
        --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [ -n "${POD:-}" ]; then
    echo "--- $(date +%T)  pod=$POD ---"
    docker exec "$K3S" kubectl -n "$NS" exec "$POD" -- cat /sys/fs/cgroup/cpu.stat 2>/dev/null \
      | grep -E 'nr_periods|nr_throttled|throttled_usec' || echo "(cpu.stat not readable yet)"
  else
    echo "--- $(date +%T)  no Running executor pod ---"
  fi
  sleep 3
done
