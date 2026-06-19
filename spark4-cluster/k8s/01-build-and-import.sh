#!/usr/bin/env bash
# Step 1 — build the app jar, bake it into a Spark image, and import that image into k3s.
#
# k3s runs its own containerd (not the host Docker daemon), so a locally-built image is
# invisible to it until imported. We import into the "k8s.io" containerd namespace, which is
# the one k3s pulls from. With imagePullPolicy=Never the pods then use this exact image and
# never hit a registry.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
K8S_DIR="$REPO_ROOT/spark4-cluster/k8s"
IMAGE="spark-cpu-demo:latest"
K3S_CONTAINER="spark4-k3s"

echo ">> [1/4] Building thin app jar (sbt package)..."
cd "$REPO_ROOT"
sbt --batch -error 'root/package'

JAR="$(ls -t "$REPO_ROOT"/target/scala-2.13/spark-performance-tuning_*.jar | head -1)"
echo ">> Built: $JAR"
cp "$JAR" "$K8S_DIR/app.jar"

echo ">> [2/4] Building image $IMAGE ..."
docker build -t "$IMAGE" "$K8S_DIR"

echo ">> [3/4] Checking k3s is running..."
if ! docker ps --format '{{.Names}}' | grep -q "^${K3S_CONTAINER}$"; then
  echo "!! $K3S_CONTAINER is not running. Start it with:"
  echo "     docker compose --profile k8s up -d spark-master k3s-server"
  exit 1
fi

echo ">> [4/4] Importing $IMAGE into k3s containerd (namespace k8s.io)..."
docker save "$IMAGE" | docker exec -i "$K3S_CONTAINER" ctr -n k8s.io images import -

echo ">> Done. Image available inside k3s as docker.io/library/$IMAGE"
