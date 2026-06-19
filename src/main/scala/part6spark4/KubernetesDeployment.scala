package part6spark4

import org.apache.spark.sql.SparkSession

object KubernetesDeployment {

  /**
    * UPDATE - New lesson for Spark 4.
    *
    * Running Spark on Kubernetes is now the dominant deployment model alongside YARN.
    * Mesos support was removed in Spark 4. This lesson covers Kubernetes-specific
    * performance tuning — how to size pods, manage shuffle data, and configure
    * dynamic allocation without an external shuffle service.
    *
    * ── RUNNABLE PROOF ──────────────────────────────────────────────────────────────
    * Section 4 (CPU requests vs limits) has a full hands-on demonstration on a real
    * single-node Kubernetes cluster running in Docker. It runs part6spark4.CpuThrottlingBenchmark
    * twice with the SAME spark.executor.cores but different CPU limits and measures the
    * wall-clock + cgroup cpu.stat difference. Step by step:
    *     spark4-cluster/k8s/README.md
    */

  val spark = SparkSession.builder()
    .appName("Kubernetes Deployment")
    .master("local[*]") // locally; on K8s this would be k8s://https://<api-server>
    .getOrCreate()

  /*
    ===========================================================================
    1. SUBMITTING TO KUBERNETES
    ===========================================================================

    spark-submit \
      --master k8s://https://<k8s-apiserver>:6443 \
      --deploy-mode cluster \
      --name my-spark-app \
      --class com.example.MyApp \
      --conf spark.kubernetes.container.image=my-registry/spark:4.1.1 \
      --conf spark.kubernetes.namespace=spark-apps \
      --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
      --conf spark.executor.instances=10 \
      --conf spark.executor.memory=8g \
      --conf spark.executor.cores=4 \
      local:///opt/spark/jars/my-app.jar

    The "local://" prefix means the jar is inside the container image.
    For remote jars: s3a://, gs://, hdfs://

    Spark 4 also ships an official Kubernetes Operator (apache/spark-kubernetes-operator)
    that provides a SparkApplication CRD for declarative management:

      helm install spark spark/spark-kubernetes-operator

    ===========================================================================
    2. EXECUTOR POD SIZING
    ===========================================================================

    The most critical performance decision on K8s is how you size executor pods.

    Fat executors (fewer, larger pods):
      + More tasks run pod-local → less shuffle over the network
      + Better for broadcast joins (broadcast table fits in one big executor)
      + Less scheduling overhead
      - Single pod failure loses more work
      - Harder to schedule on fragmented nodes

    Thin executors (many, smaller pods):
      + Better fault tolerance
      + Easier to schedule across nodes
      + Better cluster utilization
      - More shuffle traffic between pods
      - More overhead from pod creation/teardown

    Recommended starting point:
      spark.executor.cores = 4
      spark.executor.memory = 16g
      spark.kubernetes.executor.request.cores = 4
      spark.kubernetes.executor.limit.cores = 5   (slight burst headroom)

    Spark 4 change: spark.kubernetes.allocation.batch.size default is now 10 (was 5)
    — Spark creates 10 executor pods per allocation round instead of 5.

    ===========================================================================
    3. MEMORY OVERHEAD
    ===========================================================================

    Executor pod memory request = spark.executor.memory
                                + max(spark.executor.memoryOverhead, memory * memoryOverheadFactor)
                                + spark.memory.offHeap.size
                                + spark.executor.pyspark.memory

    spark.executor.memoryOverheadFactor = 0.1 (10%) for JVM, 0.4 (40%) for PySpark
    Minimum overhead is always 384 MiB.

    Spark 4 adds: spark.executor.minMemoryOverhead / spark.driver.minMemoryOverhead
    — sets a floor for memory overhead (useful when the 10% factor is too little for small heaps).

    If pods get OOM-killed by Kubernetes, increase memoryOverheadFactor to 0.15-0.2.

    ===========================================================================
    4. CPU REQUESTS vs LIMITS
    ===========================================================================

    For CPU:
      spark.kubernetes.executor.request.cores = 4   (K8s scheduling guarantee)
      spark.kubernetes.executor.limit.cores = 5     (burst ceiling)

    WARNING: Setting CPU limit = request causes K8s CPU throttling.
    Throttling is silent (no logs, no events) — the pod just gets slower.
    Best practice: set limit 1.5-2x the request, or omit it entirely.

    >> PROVE IT: spark4-cluster/k8s/README.md runs CpuThrottlingBenchmark on a real k3s node
       with limit==request (throttled) vs limit>request (burstable). Same job, ~2x slower when
       throttled, and /sys/fs/cgroup/cpu.stat nr_throttled climbs only in the throttled run.

    For memory:
      Spark sets memory request = memory limit automatically (Guaranteed QoS).
      This is intentional — Spark sets both -Xms and -Xmx, so exceeding the limit is a real OOM.

    ===========================================================================
    5. SHUFFLE DATA STORAGE ON K8S
    ===========================================================================

    There is NO external shuffle service on Kubernetes (unlike YARN).
    Shuffle data lives and dies with the executor pod. Three strategies:

    Strategy A: Local NVMe SSDs (hostPath) — fastest
      Volumes named "spark-local-dir-*" are automatically used for Spark scratch/shuffle:

      --conf spark.kubernetes.executor.volumes.hostPath.spark-local-dir-1.mount.path=/data/spark
      --conf spark.kubernetes.executor.volumes.hostPath.spark-local-dir-1.options.path=/mnt/nvme0
      --conf spark.kubernetes.executor.volumes.hostPath.spark-local-dir-1.options.type=Directory

    Strategy B: emptyDir with tmpfs — RAM-backed, fastest I/O, limited size
      --conf spark.kubernetes.local.dirs.tmpfs=true
      Counts toward pod memory — increase memoryOverheadFactor accordingly.

    Strategy C: Persistent Volume Claims — shuffle survives pod death
      --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.claimName=OnDemand
      --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.storageClass=gp3-ssd
      --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.sizeLimit=200Gi
      --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.mount.path=/data/spark
      --conf spark.shuffle.sort.io.plugin.class=org.apache.spark.shuffle.KubernetesLocalDiskShuffleDataIO

    Spark 4 change: PVCs now use ReadWriteOncePod access mode (was ReadWriteOnce).

    ===========================================================================
    6. DYNAMIC ALLOCATION ON K8S
    ===========================================================================

    Since there's no external shuffle service, you need one of these alternatives:

    Option A — Shuffle Tracking (simplest):
      spark.dynamicAllocation.enabled = true
      spark.dynamicAllocation.shuffleTracking.enabled = true
      spark.dynamicAllocation.shuffleTracking.timeout = infinity

      Keeps executors alive until their shuffle data is consumed. Simple but may hold
      executors longer than needed.

    Option B — PVC-based shuffle recovery (most robust):
      spark.dynamicAllocation.enabled = true
      spark.shuffle.sort.io.plugin.class = org.apache.spark.shuffle.KubernetesLocalDiskShuffleDataIO
      spark.kubernetes.driver.ownPersistentVolumeClaim = true
      spark.kubernetes.driver.reusePersistentVolumeClaim = true

      Shuffle data persists on PVCs — executors can be freely scaled without losing shuffle data.

    Option C — Decommissioning (for spot/preemptible instances):
      spark.dynamicAllocation.enabled = true
      spark.decommission.enabled = true
      spark.storage.decommission.shuffleBlocks.enabled = true

      Migrates shuffle blocks off executors before they terminate.

    ===========================================================================
    7. NODE AFFINITY AND SCHEDULING
    ===========================================================================

    Control where Spark pods run:
      spark.kubernetes.node.selector.disktype = ssd                            (all pods)
      spark.kubernetes.executor.node.selector.topology.kubernetes.io/zone = us-east-1a (executors only)
      spark.kubernetes.driver.node.selector.node.kubernetes.io/instance-type = m5.xlarge (driver only)

    Custom schedulers (e.g. Volcano, YuniKorn for gang scheduling):
      spark.kubernetes.scheduler.name = volcano

    Pod templates for fine-grained control:
      spark.kubernetes.executor.podTemplateFile = /path/to/executor-template.yaml

    ===========================================================================
    8. EXECUTOR ROLLING (for long-running jobs / streaming)
    ===========================================================================

    Spark can periodically replace executors to mitigate GC degradation or memory leaks:
      spark.kubernetes.executor.rollInterval = 1h
      spark.kubernetes.executor.rollPolicy = OUTLIER  (replace worst performer)
      spark.kubernetes.executor.minTasksPerExecutorBeforeRolling = 10

    Roll policies: ID, ADD_TIME, TOTAL_GC_TIME, TOTAL_DURATION, AVERAGE_DURATION, FAILED_TASKS, OUTLIER
  */

  def main(args: Array[String]): Unit = {
    println("This lesson is primarily a reference for Kubernetes deployment configurations.")
    println("Run the examples via spark-submit with --master k8s://...")
    println()
    println("Hands-on CPU-throttling proof (real k3s-in-Docker cluster):")
    println("  see spark4-cluster/k8s/README.md — runs part6spark4.CpuThrottlingBenchmark")
    println("  twice (throttled limit==request vs burstable limit>request) and compares")
    println("  wall-clock time + /sys/fs/cgroup/cpu.stat.")
    spark.stop()
  }
}
