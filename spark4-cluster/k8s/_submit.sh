#!/usr/bin/env bash
# Shared spark-submit for the throttling demo. Called by 03-submit-throttled.sh and
# 04-submit-burstable.sh — the ONLY thing that differs between them is limit.cores.
#
# The deliberate setup that makes throttling visible:
#   spark.executor.cores                  = 4   -> Spark runs 4 task threads (logical slots)
#   spark.kubernetes.executor.request.cores = 2 -> K8s only SCHEDULES the pod as 2 CPUs
#   spark.kubernetes.executor.limit.cores  = $2 -> the CFS hard cap (the knob we vary)
#
# This decoupling (4 task threads in a pod capped at <4 CPUs) is the silent pitfall: Spark
# happily schedules 4 tasks, but the kernel throttles them. No error, no log line — just a
# slower wall clock. Submit runs in cluster mode and blocks until the driver pod finishes.
set -euo pipefail

MODE="${1:?usage: _submit.sh <mode> <limit-cores>}"
LIMIT_CORES="${2:?usage: _submit.sh <mode> <limit-cores>}"

ROWS_MILLIONS="${ROWS_MILLIONS:-4}"   # job size; raise for a longer, steadier measurement
ITERS="${ITERS:-5000}"
EXEC_CORES="${EXEC_CORES:-4}"
REQUEST_CORES="${REQUEST_CORES:-2}"

echo "=================================================================="
echo " Submitting [$MODE]  executor.cores=$EXEC_CORES  request.cores=$REQUEST_CORES  limit.cores=$LIMIT_CORES"
echo "=================================================================="

docker exec -e KUBECONFIG=/kube/kubeconfig-internal.yaml spark4-master \
  /opt/spark/bin/spark-submit \
  --master k8s://https://k3s-server:6443 \
  --deploy-mode cluster \
  --name "cpu-throttle-$MODE" \
  --class part6spark4.CpuThrottlingBenchmark \
  --conf spark.kubernetes.namespace=spark-demo \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.container.image=spark-cpu-demo:latest \
  --conf spark.kubernetes.container.image.pullPolicy=Never \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores="$EXEC_CORES" \
  --conf spark.kubernetes.executor.request.cores="$REQUEST_CORES" \
  --conf spark.kubernetes.executor.limit.cores="$LIMIT_CORES" \
  --conf spark.executor.memory=1g \
  --conf spark.driver.memory=1g \
  --conf spark.driver.cores=1 \
  --conf spark.kubernetes.executor.deleteOnTermination=false \
  local:///opt/spark-apps/app.jar "$ROWS_MILLIONS" "$ITERS"

echo
echo ">> Driver pod kept (Completed). Read its printed wall-clock with:"
echo "     ./05-results.sh"
