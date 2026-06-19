# spark4-cluster

Docker-based infrastructure for the **Part 6 (Spark 4)** demos. Three things live here, all
built on the official `apache/spark:4.1.1-scala` image:

| Service | Purpose | Default? |
|---------|---------|----------|
| `spark-master` + `spark-worker` | classic standalone cluster (UI on http://localhost:9090) | yes |
| `spark-connect` | Spark Connect gRPC server on `:15002` | yes |
| `k3s-server` | single-node Kubernetes, for the CPU-throttling demo | no (profile `k8s`) |

---

## A. Spark Connect server (for `connect/SparkConnectPerformance`)

```bash
cd spark4-cluster
docker compose up -d spark-master spark-worker spark-connect
```

Wait until the connect container logs `Spark Connect server started`, then from the repo root:

```bash
sbt "connect/runMain part6spark4.SparkConnectPerformance"
```

The client connects to `sc://localhost:15002` (override with `SPARK_REMOTE`). It measures the
Connect-specific performance surface: plan caching, schema-RPC overhead, and Arrow result
transfer. See the comments in `SparkConnectPerformance.scala` for what each number means.

Tear down:

```bash
docker compose down
```

## B. Kubernetes CPU-throttling demo

This one has its own profile (so it never starts by accident) and its own step-by-step guide:

```bash
docker compose --profile k8s up -d spark-master k3s-server
```

Then follow **[k8s/README.md](k8s/README.md)** — it walks through building the image, importing
it into k3s, and submitting the same job twice (throttled vs burstable) to prove silent CPU
throttling with wall-clock time and `cpu.stat`.

---

## Layout

```
spark4-cluster/
├── docker-compose.yml      # master + worker + connect, and k3s under the "k8s" profile
├── spark-apps/             # mounted into the standalone containers (/opt/spark-apps)
├── spark-data/             # mounted into the standalone containers (/opt/spark-data)
└── k8s/
    ├── README.md           # the CPU-throttling step-by-step
    ├── Dockerfile          # apache/spark + our thin app jar
    ├── spark-rbac.yaml     # namespace + service account so the driver can spawn executors
    ├── 01-build-and-import.sh
    ├── 02-setup.sh
    ├── 03-submit-throttled.sh
    ├── 04-submit-burstable.sh
    ├── 05-watch-throttling.sh
    ├── 06-results.sh
    └── 99-cleanup.sh
```
