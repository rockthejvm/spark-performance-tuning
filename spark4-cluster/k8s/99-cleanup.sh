#!/usr/bin/env bash
# Delete all driver/executor pods from the demo namespace (keeps the namespace + RBAC).
# To tear the whole Kubernetes node down:  docker compose --profile k8s down -v
set -euo pipefail
K3S=spark4-k3s
NS=spark-demo
docker exec "$K3S" kubectl -n "$NS" delete pods --all --ignore-not-found
echo ">> Cleared pods in namespace '$NS'."
