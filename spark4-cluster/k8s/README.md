# Spark on Kubernetes — the silent CPU-throttling pitfall (step by step)

This is the runnable proof for section 4 of `part6spark4/KubernetesDeployment.scala`:

> **Setting CPU limit = request causes K8s CPU throttling. Throttling is silent (no logs, no
> events) — the pod just gets slower.**

We run the **exact same** CPU-bound Spark job twice on a real (single-node) Kubernetes cluster.
The only thing that changes between the two runs is the executor pod's CPU **limit**. The
wall-clock time and the kernel's `cpu.stat` counters prove the difference.

The job is `part6spark4.CpuThrottlingBenchmark`: a tight floating-point loop with **no shuffle
and no I/O**, so the only thing limiting throughput is how many CPU cycles the pod is allowed.

---

## Why this configuration throttles

| conf | value | meaning |
|------|-------|---------|
| `spark.executor.cores` | `4` | Spark runs **4 task threads** in the executor |
| `spark.kubernetes.executor.request.cores` | `2` | K8s **schedules** the pod as if it needs 2 CPUs |
| `spark.kubernetes.executor.limit.cores` | **the knob** | the kernel CFS **hard cap** |

- **Throttled run** (`limit.cores = 2`, i.e. `== request`): Guaranteed QoS, zero burst room.
  4 task threads are squeezed into a 2-CPU quota → roughly **2× slower**.
- **Burstable run** (`limit.cores = 4`, i.e. `> request`): the 4 threads get 4 CPUs of quota
  → **full speed**.

The trap in the real world: `spark.executor.cores` (a *logical* task-slot count) and the pod's
CPU *quota* are configured in different places by different people. When the quota is below the
thread count, every task is throttled and **nothing logs it**.

---

## Prerequisites

- Docker + Docker Compose
- A host with **at least 4 free CPU cores** (so the burstable run can actually burst)
- This repo built with `sbt` (the scripts call `sbt package` for you)

All commands below run from `spark4-cluster/`.

---

## Steps

### 0. Start the Kubernetes node + the submitter

```bash
cd spark4-cluster
docker compose --profile k8s up -d spark-master k3s-server
```

- `k3s-server` is a one-node Kubernetes cluster running inside Docker.
- `spark-master` is reused purely as the **submitter** — it already contains `spark-submit`
  4.1.1 and reaches the k3s API over the compose network at `https://k3s-server:6443`.

### 1. Build the app image and import it into k3s

```bash
./k8s/01-build-and-import.sh
```

`sbt package` → bake the thin jar into `spark-cpu-demo:latest` → import that image into k3s's
own containerd (k3s can't see host Docker images otherwise). Pods use it with
`imagePullPolicy=Never`, so there is no registry involved.

### 2. Create the namespace, RBAC, and submitter kubeconfig

```bash
./k8s/02-setup.sh
```

Waits for the node to be `Ready`, applies `spark-rbac.yaml` (the driver pod needs rights to
create executor pods), and rewrites the kubeconfig's server URL to `https://k3s-server:6443`
for the submitter container.

### 3. Run the THROTTLED job, watching throttling live

In **terminal A**:

```bash
./k8s/03-submit-throttled.sh      # limit.cores == request.cores == 2  (blocks until done)
```

In **terminal B**, while A is running:

```bash
./k8s/05-watch-throttling.sh      # samples the executor pod's cpu.stat every 3s
```

You'll see `nr_throttled` and `throttled_usec` climb steadily — the kernel is parking the task
threads because they've used their 2-CPU quota for the period.

### 4. Run the BURSTABLE job

```bash
./k8s/04-submit-burstable.sh      # limit.cores (4) > request.cores (2)
```

Re-run `05-watch-throttling.sh` alongside it: `nr_throttled` stays essentially flat.

### 5. Compare the results

```bash
./k8s/06-results.sh
```

This reads the `Wall-clock time` / `Throughput` lines the benchmark printed in each driver pod.
Expect the **throttled** run to be markedly slower than the **burstable** one — with **no error
or warning** to explain it. That silence is the entire lesson.

### 6. Cleanup

```bash
./k8s/99-cleanup.sh                       # delete demo pods
docker compose --profile k8s down -v      # tear down the whole k3s node
```

---

## What you just proved

1. **Wall-clock**: identical job, identical `spark.executor.cores`, ~2× slower purely from the
   CPU limit.
2. **`cpu.stat`**: `nr_throttled` rises only in the throttled run — the kernel-level cause.
3. **Silence**: Spark emitted no warning. This is why the lesson says to set
   `limit.cores` ≥ `1.5–2× request`, or omit it entirely.

### Tuning the run length

The scripts honour environment variables (defaults in parentheses):

```bash
ROWS_MILLIONS=8 ITERS=8000 ./k8s/03-submit-throttled.sh   # bigger/longer (4, 5000)
```

Make the unthrottled run last ≥ ~30s so pod startup doesn't dominate the comparison.
