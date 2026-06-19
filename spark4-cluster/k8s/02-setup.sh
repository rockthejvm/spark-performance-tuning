#!/usr/bin/env bash
# Step 2 — create the namespace + RBAC, and produce a kubeconfig the spark-master container
# can use to reach the API server.
#
# k3s writes its kubeconfig pointing at https://127.0.0.1:6443 (correct for the HOST). The
# spark-master container reaches the API over the compose network at https://k3s-server:6443,
# so we rewrite the server URL into kubeconfig-internal.yaml. The API cert is valid for that
# name because k3s was started with --tls-san k3s-server.
set -euo pipefail

K8S_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KUBECFG_DIR="$K8S_DIR/kubeconfig"
K3S_CONTAINER="spark4-k3s"

echo ">> Waiting for k3s to write its kubeconfig..."
for _ in $(seq 1 30); do
  [ -f "$KUBECFG_DIR/kubeconfig.yaml" ] && break
  sleep 2
done
if [ ! -f "$KUBECFG_DIR/kubeconfig.yaml" ]; then
  echo "!! k3s kubeconfig never appeared. Is k3s-server up?"
  exit 1
fi

echo ">> Waiting for the node to be Ready..."
until docker exec "$K3S_CONTAINER" kubectl get nodes 2>/dev/null | grep -q " Ready "; do
  sleep 2
done
docker exec "$K3S_CONTAINER" kubectl get nodes

echo ">> Writing kubeconfig-internal.yaml (server -> https://k3s-server:6443)..."
sed 's#https://127.0.0.1:6443#https://k3s-server:6443#' \
  "$KUBECFG_DIR/kubeconfig.yaml" > "$KUBECFG_DIR/kubeconfig-internal.yaml"

echo ">> Applying namespace + RBAC..."
docker exec -i "$K3S_CONTAINER" kubectl apply -f - < "$K8S_DIR/spark-rbac.yaml"

echo ">> Setup complete. Namespace 'spark-demo' and service account 'spark' are ready."
