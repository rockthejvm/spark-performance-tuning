# Video Recording Plan — Spark 4 Upgrade

Every Scala file is one video. This document covers every video in the course:
what to keep as-is, what to patch (add a short segment), and what to record fresh.

---

## Videos to Remove

None. Every existing lesson remains valid on Spark 4.

---

## Videos That Need No Changes (keep as-is)

These files have zero code changes. The concepts they teach are identical on Spark 4.
No patching, no re-recording.

| # | Video (course section) | File | Why it's fine |
|---|---|---|---|
| 1 | Welcome & Setup | *(no file)* | Re-record this one separately — see below |
| 2 | Scala Recap | `ScalaRecap.scala` | Pure Scala, no Spark APIs affected |
| 3 | Spark Job Anatomy | `SparkJobAnatomy.scala` | Jobs, stages, tasks — unchanged in Spark 4 |
| 4 | The Spark UI and DAGs | `ReadingDAGs.scala` | DAG visualization is the same |
| 5 | Tungsten | `TungstenDemo.scala` | WholeStageCodegen works identically |
| 6 | Checkpointing | `Checkpointing.scala` | checkpoint() API unchanged |
| 7 | Repartition & Coalesce | `RepartitionCoalesce.scala` | repartition/coalesce unchanged |
| 8 | Custom Partitioners | `Partitioners.scala` | HashPartitioner, RangePartitioner, custom — unchanged |
| 9 | Serialization Problems 1 & 2 | `SerializationProblems.scala` | Closure serialization unchanged |

---

## Videos to Patch

"Patch" = keep the existing recording, splice in a short segment (30–90 seconds)
at a specific point. The code has an `UPDATE` comment marking exactly where.

### Patch 1 — Spark Recap

| | |
|---|---|
| **File** | `SparkRecap.scala` |
| **Course video** | Spark Recap (Section 1) |
| **Where to splice** | After showing `spark.sql(...)` and Spark SQL, ~line 73 |
| **What to say** | *"One thing that changed in Spark 4: `spark.sql.ansi.enabled` is now true by default. Division by zero throws a `DIVIDE_BY_ZERO` exception instead of returning null. Cast overflows throw `CAST_OVERFLOW` instead of wrapping. Use `try_divide`, `try_cast` for the old behavior. This applies everywhere — Spark SQL, DataFrames, Datasets."* |
| **Duration** | ~30 seconds |

### Patch 2 — Query Plans

| | |
|---|---|
| **File** | `ReadingQueryPlans.scala` |
| **Course video** | Query Plans (Section 2) |
| **Where to splice** | At the very beginning, before showing any `explain()` output |
| **What to say** | *"Quick note: the query plan output in the comments was captured on Spark 3. Spark 4 has AQE improvements and plan format changes, so your output will look slightly different. The structure is the same — read them the same way."* |
| **Duration** | ~15 seconds |

### Patch 3 — A Tale of Two Spark APIs

| | |
|---|---|
| **File** | `SparkAPIs.scala` |
| **Course video** | A Tale of Two Spark APIs (Section 2) |
| **Where to splice** | At the very beginning, before the benchmark comparisons |
| **What to say** | *"The timings in this lesson were measured on JDK 8. JDK 17 with G1GC changes the performance profile, so your absolute numbers will differ. The relative comparisons — RDDs vs DataFrames vs Datasets — still hold. Also, the inline query plans were captured on Spark 3. Run `explain()` yourself to see the Spark 4 output — the teaching point is the same: lambdas force `DeserializeToObject` and prevent Catalyst optimization."* |
| **Duration** | ~30 seconds |

### Patch 4 — Deploying & Configuring Spark Applications

| | |
|---|---|
| **File** | `TestDeployApp.scala` |
| **Course video** | Deploying & Configuring Spark Applications (Section 2) |
| **Where to splice** | After showing `spark-submit --conf` options, ~line 46 |
| **What to say** | *"Spark 4 requires JDK 17 or newer. The biggest implication: CMS garbage collector was removed in JDK 14. If your deploy scripts use `-XX:+UseConcMarkSweepGC`, the JVM will refuse to start. G1GC is the new default. For GC tuning, use G1GC flags: `-XX:G1HeapRegionSize=16m`, `-XX:InitiatingHeapOccupancyPercent=35`. We'll cover G1GC tuning in depth in a dedicated lesson."* |
| **Duration** | ~45 seconds |

### Patch 5 — Catalyst

| | |
|---|---|
| **File** | `CatalystDemo.scala` |
| **Course video** | Catalyst (Section 2) |
| **Where to splice** | After explaining filter pushdown and chained filter optimization, ~line 35 |
| **What to say** | *"Spark 4 improved Catalyst further. Multi-key dynamic partition pruning now broadcasts multiple filtering keys simultaneously — great for star-schema queries. And multi-layer runtime filters push down predicates at more layers of the query plan. The optimizer is more aggressive now."* |
| **Duration** | ~30 seconds |

### Patch 6 — Caching

| | |
|---|---|
| **File** | `Caching.scala` |
| **Course video** | Caching (Section 3) |
| **Where to splice** | When discussing `StorageLevel.OFF_HEAP`, ~line 38 |
| **What to say** | *"On JDK 17 with G1GC — which is the default in Spark 4 — off-heap caching is even more attractive. It avoids G1's pause-time overhead on large heaps. If your executors have 32GB+ heaps and you're seeing long GC pauses, off-heap is worth trying."* |
| **Duration** | ~20 seconds |

### Patch 7 — Partitioning Problems

| | |
|---|---|
| **File** | `PartitioningProblems.scala` |
| **Course video** | Partitioning Problems (Section 4) |
| **Where to splice** | After the "10-100MB rule for partition size" discussion, ~line 54 |
| **What to say** | *"Spark 4 now enforces this at the engine level. `spark.sql.maxSinglePartitionBytes` defaults to 128MB — partitions that previously could grow unbounded are now capped automatically. This means more partitions and higher parallelism for jobs that previously ran with oversized partitions. You can override it, but the default aligns with the rule we just discussed."* |
| **Duration** | ~30 seconds |

### Patch 8 — Fixing Data Skews

| | |
|---|---|
| **File** | `FixingDataSkews.scala` |
| **Course video** | Fixing Data Skews and Straggling Tasks (Section 5) |
| **Where to splice** | Before showing the salting solution, ~line 31 |
| **What to say** | *"Before we get into manual salting — Spark's AQE now detects skewed partitions at runtime and splits them automatically. `spark.sql.adaptive.skewJoin.enabled` is on by default. AQE handles the common case. Manual salting is still needed when the skew emerges after a non-equi condition, when skew is in aggregations or window functions rather than joins, or when you need deterministic partition control — which is exactly the scenario we have here."* |
| **Duration** | ~45 seconds |

### Patch 9 — Kryo Serializer

| | |
|---|---|
| **File** | `KryoSerializer.scala` |
| **Course video** | Kryo (Section 5) |
| **Where to splice** | Before showing the benchmark numbers, ~line 32 |
| **What to say** | *"These benchmarks were measured on JDK 8 with Spark 3. The absolute numbers will differ on JDK 17 with G1GC — re-run them on your setup. What stays the same: Kryo's relative advantage over Java serialization. You'll still see ~35% less memory for caching and ~40% smaller shuffle sizes."* |
| **Duration** | ~20 seconds |

---

## Videos to Re-record

These need a full re-record, not a patch.

### Re-record — Welcome & Setup

| | |
|---|---|
| **Course video** | Welcome & Setup (Section 1) |
| **Why re-record** | The entire environment changed: Spark 4.1.1, Scala 2.13.17, JDK 17+, new build.sbt, removed dead resolvers. Students need to set up from scratch. |
| **What to cover** | Show the new `build.sbt`. Mention JDK 17+ requirement. Walk through project structure including the new `part6spark4` package. Show SBT compile succeeding. |

---

## New Videos to Record

Five new lessons, all in `src/main/scala/part6spark4/`. These are Section 6 of the course.

### New 1 — JDK 17+ and G1GC Tuning

| | |
|---|---|
| **File** | `G1GCTuning.scala` |
| **Estimated duration** | 20–25 min |
| **Demo** | Run `demonstrateGCImpact()`, open Spark UI → Executors → GC Time column |
| **Key points** | Why JDK 17 (CMS gone). How G1GC works (regions, pause-time goals). 6 tuning params: `G1HeapRegionSize`, `InitiatingHeapOccupancyPercent`, `G1ReservePercent`, `MaxGCPauseMillis`, `ParallelGCThreads`, `ConcGCThreads`. JDK 17 `-Xlog:gc*` format. Rule: GC Time > 10% of task time → tune. 4 scenarios (large shuffle, frequent full GCs, long pauses, streaming latency). |

### New 2 — Spark 4 Default Configuration Changes

| | |
|---|---|
| **File** | `Spark4Defaults.scala` |
| **Estimated duration** | 15–20 min |
| **Demo** | Run `ansiModeDemo()` (division by zero throws → fix with `try_divide`). Run `showCurrentDefaults()` to print all values. |
| **Key points** | Walk through 7 changed defaults: ANSI mode, `maxSinglePartitionBytes`, ORC compression (snappy→zstd), speculation (less aggressive), shuffle DB (ROCKSDB), event log compression, Prometheus metrics. For each: what changed, why, when to override. |

### New 3 — Parallel Shuffle Compression

| | |
|---|---|
| **File** | `ParallelShuffleCompression.scala` |
| **Estimated duration** | 15–20 min |
| **Demo** | Run `benchmarkCodec("lz4")`, `"snappy"`, `"zstd"`. Compare console times. Open Spark UI → Stages → compare Shuffle Write/Read sizes. |
| **Key points** | ZSTD and LZF now compress in parallel (was single-threaded). Codec comparison table. `spark.io.compression.zstd.workers` (set > 0 for parallel). When to use ZSTD (I/O-bound, cloud, large shuffles) vs LZ4 (CPU-bound, small shuffles). |

### New 4 — Spark on Kubernetes

| | |
|---|---|
| **File** | `KubernetesDeployment.scala` |
| **Estimated duration** | 25–30 min |
| **Demo** | Reference-style — show commands and configs on screen, optionally live on a cluster |
| **Key points** | 8 sections: `spark-submit --master k8s://`, official K8s Operator, pod sizing (fat vs thin), memory overhead formula, CPU requests vs limits (silent throttling warning), shuffle storage (3 strategies: hostPath/tmpfs/PVC), dynamic allocation without shuffle service (3 options: tracking/PVC/decommissioning), node affinity, executor rolling. |

### New 5 — Spark Connect Performance

| | |
|---|---|
| **File** | `SparkConnectPerformance.scala` |
| **Estimated duration** | 20–25 min |
| **Demo** | Run `demonstratePlanCaching()` (toggle cache, show time diff). Run `demonstrateSchemaOverhead()` (N RPCs vs 1). Run `demonstrateCollectOverhead()` (cost scaling, then aggregate-first fix). |
| **Key points** | Architecture (protobuf plans → gRPC → server → Arrow results). No RDD API. Plan caching (22x improvement). Schema RPC anti-pattern. Result transfer costs. Plan compression (Spark 4.1). When Connect vs Classic. Config reference table. |

---

## Summary Table

| Video | File | Action | Est. Work |
|---|---|---|---|
| Welcome & Setup | *(build.sbt)* | **Re-record** | Full video |
| Scala Recap | `ScalaRecap.scala` | Keep as-is | — |
| Spark Recap | `SparkRecap.scala` | **Patch** ~30s | ANSI mode note |
| Core | *(slides)* | Keep as-is | — |
| Spark Job Anatomy | `SparkJobAnatomy.scala` | Keep as-is | — |
| Query Plans | `ReadingQueryPlans.scala` | **Patch** ~15s | Plan format note |
| The Spark UI and DAGs | `ReadingDAGs.scala` | Keep as-is | — |
| A Tale of Two Spark APIs | `SparkAPIs.scala` | **Patch** ~30s | Stale timings note |
| Deploy & Configure | `TestDeployApp.scala` | **Patch** ~45s | G1GC flags |
| Catalyst | `CatalystDemo.scala` | **Patch** ~30s | Improved DPP |
| Tungsten | `TungstenDemo.scala` | Keep as-is | — |
| Executor Memory Arch. | *(slides)* | Keep as-is | — |
| Caching | `Caching.scala` | **Patch** ~20s | Off-heap + G1GC |
| Checkpointing | `Checkpointing.scala` | Keep as-is | — |
| Repartition & Coalesce | `RepartitionCoalesce.scala` | Keep as-is | — |
| Partitioning Problems | `PartitioningProblems.scala` | **Patch** ~30s | maxSinglePartitionBytes |
| Shuffle Partitioning | *(slides)* | Keep as-is | — |
| Custom Partitioners | `Partitioners.scala` | Keep as-is | — |
| Cluster Resource Alloc. | *(slides)* | Keep as-is | — |
| Fixing Data Skews | `FixingDataSkews.scala` | **Patch** ~45s | AQE skew join |
| Serialization Problems 1 | `SerializationProblems.scala` | Keep as-is | — |
| Serialization Problems 2 | `SerializationProblems.scala` | Keep as-is | — |
| Kryo | `KryoSerializer.scala` | **Patch** ~20s | Stale benchmarks |
| G1GC Tuning | `G1GCTuning.scala` | **Record new** | ~20–25 min |
| Spark 4 Default Configs | `Spark4Defaults.scala` | **Record new** | ~15–20 min |
| Parallel Shuffle Compression | `ParallelShuffleCompression.scala` | **Record new** | ~15–20 min |
| Spark on Kubernetes | `KubernetesDeployment.scala` | **Record new** | ~25–30 min |
| Spark Connect Performance | `SparkConnectPerformance.scala` | **Record new** | ~20–25 min |
| You ROCK! | *(outro)* | Keep as-is | — |

**Totals:**
- Keep as-is: 13 videos
- Patch (short splice): 9 videos (~4.5 min total new footage)
- Re-record: 1 video (Welcome & Setup)
- Record new: 5 videos (~95–120 min total new footage)
- Remove: 0 videos
