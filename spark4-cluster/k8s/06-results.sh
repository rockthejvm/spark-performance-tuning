#!/usr/bin/env bash
# Step 6 — pull the wall-clock numbers the benchmark printed, from each driver pod's logs.
# Compare the throttled vs burstable "Wall-clock time" — that difference is the headline proof.
set -euo pipefail
K3S=spark4-k3s
NS=spark-demo

echo ">> Driver pods:"
docker exec "$K3S" kubectl -n "$NS" get pods -l spark-role=driver

for POD in $(docker exec "$K3S" kubectl -n "$NS" get pods -l spark-role=driver \
             -o jsonpath='{.items[*].metadata.name}'); do
  echo
  echo "================ $POD ================"
  docker exec "$K3S" kubectl -n "$NS" logs "$POD" 2>/dev/null \
    | grep -E 'executor.cores|CPU-bound benchmark|Wall-clock|Throughput' \
    || echo "(no benchmark lines — job may still be running)"
done

echo
echo ">> Compare 'Wall-clock time': the throttled run should be markedly slower than burstable,"
echo "   with NO error or warning to explain it. That silence is the whole point of the lesson."
