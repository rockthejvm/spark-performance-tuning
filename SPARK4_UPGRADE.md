# Spark 4 Upgrade — Course Update Plan

Course: **Apache Spark Performance Tuning with Scala** (Rock the JVM)
Upgrade: Spark 3.5.0 → **Spark 4.1.1**, Scala 2.13.12 → 2.13.17, JDK 8+ → JDK 17+

---

## Build Changes

| Dependency | Old | New |
|---|---|---|
| Spark | 3.5.0 | 4.1.1 |
| Scala | 2.13.12 | 2.13.17 |
| log4j | 2.20.0 | 2.24.3 |
| JDK minimum | 8 | **17** |

Removed dead resolvers (bintray, typesafe, mvnrepository) and unused `postgresVersion`.

---

## Existing Lessons — Recording Annotations

These lessons still work and teach the same concepts, but need brief verbal callouts during recording or on-screen annotations. Each UPDATE comment in the code marks the exact spot.

### Part 1: Recap

**SparkRecap.scala** — Mention ANSI mode
- After showing Spark SQL, note: *"In Spark 4, `spark.sql.ansi.enabled` is true by default. Division by zero now throws an exception instead of returning null, and cast overflows throw instead of wrapping silently. Use `try_divide`, `try_cast` etc. for the old behavior."*

### Part 2: Foundations

**TestDeployApp.scala** — GC flags are different
- When discussing `spark-submit --conf`, add: *"Spark 4 requires JDK 17+, which means CMS is gone — it was removed in JDK 14. G1GC is the new default. If your old deploy scripts use `-XX:+UseConcMarkSweepGC`, the JVM will refuse to start. Use G1GC flags instead: `-XX:G1HeapRegionSize`, `-XX:InitiatingHeapOccupancyPercent`."*

**ReadingQueryPlans.scala** — Plan format changed
- Before showing `explain()` output, note: *"The query plans you see here were captured on Spark 3. Spark 4 has AQE improvements and formatting changes, so your output will look slightly different. The structure and reading technique are the same."*

**SparkAPIs.scala** — Timings and plans stale
- At the start of the lesson, note: *"The timings in this lesson were measured on JDK 8. JDK 17 with G1GC changes the performance profile, so your numbers will differ. The relative comparisons — RDDs vs DataFrames vs Datasets — still hold."*
- The inline query plans in comments are from Spark 3. Mention this when showing them. The teaching point (lambdas defeat Catalyst optimization) is unchanged.

**CatalystDemo.scala** — Improved Catalyst in Spark 4
- After explaining filter pushdown, add: *"Spark 4 improves Catalyst with multi-key dynamic partition pruning and multi-layer runtime filters. Even more filters are pushed down automatically now."*

### Part 3: Caching

**Caching.scala** — Off-heap + G1GC
- When discussing `StorageLevel.OFF_HEAP`, add: *"On JDK 17 with G1GC, off-heap caching is even more attractive because it avoids G1 pause-time overhead on large heaps."*

### Part 4: Partitioning

**PartitioningProblems.scala** — New partition size cap
- After the "10-100MB rule" discussion, add: *"Spark 4 now enforces this at the engine level with `spark.sql.maxSinglePartitionBytes`, which defaults to 128MB. Partitions that previously grew unbounded are now automatically capped."*

### Part 5: Boost

**FixingDataSkews.scala** — AQE handles skew automatically
- Before showing the salting solution, add: *"Spark 4's AQE detects skewed partitions at runtime and splits them automatically via `spark.sql.adaptive.skewJoin.enabled` (on by default). Manual salting is still needed when the skew emerges after a non-equi join condition, when skew is in aggregations or window functions, or when you need deterministic partition control."*

**KryoSerializer.scala** — Benchmark numbers stale
- Note: *"These benchmarks were measured on JDK 8. Re-run them on your setup — the absolute numbers will differ on JDK 17, but Kryo's relative advantage over Java serialization still holds."*

---

## New Lessons to Record — Part 6: Spark 4

Five new lessons in the `part6spark4` package.

### Lesson 1: G1GCTuning.scala — JDK 17+ and G1GC Tuning

**What to cover:**
- Why JDK 17 is required (CMS removed in JDK 14, Parallel GC no longer default)
- How G1GC works: heap divided into regions, collects "garbage-first" regions, targets pause-time goals
- Key tuning parameters:
  - `-XX:G1HeapRegionSize` (aim for ~2048 regions)
  - `-XX:InitiatingHeapOccupancyPercent` (lower = earlier GC, default 45, try 35 for shuffle-heavy)
  - `-XX:G1ReservePercent` (increase if "to-space exhausted" appears)
  - `-XX:MaxGCPauseMillis` (200ms default, lower for streaming)
- JDK 17 unified GC logging: `-Xlog:gc*:file=gc.log` (replaces old `-XX:+PrintGCDetails`)
- Demo: run a shuffle-heavy workload and observe GC Time in the Spark UI Executor tab
- Rule of thumb: if GC Time > 10% of task time, tuning is needed

**Suggested demo flow:**
1. Run `demonstrateGCImpact()` with default G1GC settings
2. Show GC Time in Spark UI
3. Discuss when to increase region size, lower IHOP, or switch to off-heap

### Lesson 2: Spark4Defaults.scala — Changed Default Configurations

**What to cover:**
- Walk through the 7 most impactful default changes, one by one:
  1. `spark.sql.ansi.enabled = true` — live demo of division by zero throwing, then `try_divide` fix
  2. `spark.sql.maxSinglePartitionBytes = 128m` — explain the old unlimited behavior
  3. `spark.sql.orc.compression.codec = zstd` — compression ratio vs CPU tradeoff
  4. `spark.speculation.multiplier` and `quantile` — less aggressive speculation
  5. `spark.shuffle.service.db.backend = ROCKSDB`
  6. Event log compression now on by default
  7. Prometheus metrics now on by default
- Run `showCurrentDefaults()` to print all values live
- Discuss when to override vs accept the new defaults

**Suggested demo flow:**
1. Run `ansiModeDemo()` — show the exception, then the `try_divide` fix
2. Run `showCurrentDefaults()` — walk through each printed value
3. Discuss migration: which defaults to keep, which to revert

### Lesson 3: ParallelShuffleCompression.scala — Parallel Shuffle Compression

**What to cover:**
- Shuffle compression was single-threaded before Spark 4 — bottleneck on multi-core executors
- ZSTD and LZF now run compression in parallel
- Codec comparison table (in the code): lz4 vs snappy vs zstd vs lzf
- Key config: `spark.io.compression.zstd.workers` (set > 0 to enable parallel threads)
- When to use ZSTD: I/O-bound, cloud storage, large shuffles
- When to stick with LZ4: CPU-constrained, small shuffles

**Suggested demo flow:**
1. Run `benchmarkCodec("lz4")`, then `"snappy"`, then `"zstd"`
2. Compare times printed to console
3. Open Spark UI → Stages → compare Shuffle Write / Shuffle Read sizes across runs
4. Discuss the tradeoff: ZSTD compresses ~30% better but uses more CPU

### Lesson 4: KubernetesDeployment.scala — Spark on Kubernetes

**What to cover:**
- Mesos removed in Spark 4 — K8s is now the primary non-YARN deployment target
- `spark-submit --master k8s://...` syntax
- Official Spark Kubernetes Operator (new in Spark 4)
- **Pod sizing** — fat vs thin executors, recommended starting point (4 cores, 16GB)
- **Memory overhead** — the formula, `memoryOverheadFactor`, new `minMemoryOverhead` in Spark 4
- **CPU requests vs limits** — warning about silent CPU throttling
- **Shuffle storage** — the 3 strategies:
  - hostPath with local NVMe (fastest)
  - emptyDir tmpfs (RAM-backed)
  - PVCs (survives pod death)
- **Dynamic allocation without shuffle service** — the 3 options:
  - Shuffle tracking (simplest)
  - PVC-based shuffle recovery (most robust)
  - Decommissioning (for spot instances)
- **Node affinity** — node selectors, custom schedulers (Volcano/YuniKorn)
- **Executor rolling** — periodic replacement for long-running/streaming jobs

**Suggested demo flow:**
1. Show the spark-submit command for K8s (on screen, or live if you have a cluster)
2. Walk through the pod sizing decision tree
3. Show a pod template YAML for executor tuning
4. Discuss each shuffle storage strategy with a diagram
5. Walk through dynamic allocation options

### Lesson 5: SparkConnectPerformance.scala — Spark Connect Performance

**What to cover:**
- Spark Connect architecture: client builds protobuf plans, sends via gRPC, server resolves/executes, results return as Arrow batches. All Catalyst/Tungsten/AQE optimizations work identically once the plan reaches the server.
- No RDD API — DataFrame and SQL only. Forces the DataFrame-first patterns taught in Part 2.
- **Plan caching** (SPARK-47818) — the biggest win. Iterative DataFrame construction without caching re-analyzes the entire plan tree on every action. With caching: 22x improvement (110s → 5s in benchmarks). Configs: `spark.connect.session.planCache.enabled`, `maxSize` (32).
- **Schema RPC overhead** — `df.schema` and `df.columns` trigger an AnalyzePlan RPC each time. Show the anti-pattern (schema access in a loop = N round-trips) vs the fix (cache in a local val = 1 round-trip).
- **Result set transfer** — Arrow is always active (no opt-in needed). `collect()` on large DataFrames must serialize and transfer every row over gRPC. Show the cost scaling with 1K, 1M, 10M rows. The fix: aggregate server-side, collect only the result.
- **Plan compression** (Spark 4.1+) — ZSTD compression for plans > 10 MB. `spark.connect.session.planCompression.threshold`.
- **When to use Connect vs Classic** — multi-tenant/notebooks/CI vs single-tenant batch, thin client (~200MB) vs full driver (1.5GB+).
- Key configs reference table (all in the code).

**Suggested demo flow:**
1. Create the session with `spark.api.mode = "connect"` and explain what's happening
2. Run `demonstratePlanCaching()` — toggle cache on/off, show the time difference
3. Run `demonstrateSchemaOverhead()` — show N RPCs vs 1 RPC
4. Run `demonstrateCollectOverhead()` — show cost scaling, then the aggregate-first fix
5. Walk through the configuration reference table
6. Discuss when to use Connect vs Classic

---

## Checklist

### Code changes (done)
- [x] `build.sbt` — version bump
- [x] `SparkRecap.scala` — ANSI mode note
- [x] `TestDeployApp.scala` — G1GC flags
- [x] `ReadingQueryPlans.scala` — stale plans note
- [x] `SparkAPIs.scala` — stale timings + plans note
- [x] `CatalystDemo.scala` — improved Catalyst note
- [x] `Caching.scala` — off-heap + G1GC note
- [x] `PartitioningProblems.scala` — maxSinglePartitionBytes
- [x] `FixingDataSkews.scala` — AQE skew join note
- [x] `KryoSerializer.scala` — stale benchmarks note
- [x] `G1GCTuning.scala` — new lesson
- [x] `Spark4Defaults.scala` — new lesson
- [x] `ParallelShuffleCompression.scala` — new lesson
- [x] `KubernetesDeployment.scala` — new lesson
- [x] `SparkConnectPerformance.scala` — new lesson

### Recording tasks
- [ ] Re-record or annotate Part 1 recap (SparkRecap) — brief ANSI callout
- [ ] Re-record or annotate Part 2 deploy lesson — GC flags section
- [ ] Re-record or annotate Part 2 query plans — note about changed output
- [ ] Re-record or annotate Part 2 Spark APIs — note about stale timings
- [ ] Re-record or annotate Part 2 Catalyst — mention improved DPP
- [ ] Re-record or annotate Part 3 caching — off-heap + G1GC note
- [ ] Re-record or annotate Part 4 partitioning — maxSinglePartitionBytes
- [ ] Re-record or annotate Part 5 data skews — AQE skew handling
- [ ] Re-record or annotate Part 5 Kryo — stale benchmarks note
- [ ] Record new lesson: G1GC Tuning
- [ ] Record new lesson: Spark 4 Default Configs
- [ ] Record new lesson: Parallel Shuffle Compression
- [ ] Record new lesson: Kubernetes Deployment
- [ ] Record new lesson: Spark Connect Performance
- [ ] Update Dockerfile for JDK 17+
- [ ] Update course description on website (Spark 4, JDK 17+)
- [ ] Re-run all demos on Spark 4.1.1 and capture new Spark UI screenshots
