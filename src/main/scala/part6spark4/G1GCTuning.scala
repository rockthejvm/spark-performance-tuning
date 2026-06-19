package part6spark4

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import java.lang.management.ManagementFactory
import scala.jdk.CollectionConverters._

object G1GCTuning {

  /**
    * UPDATE - New lesson for Spark 4.
    *
    * Spark 4 requires JDK 17+ and defaults to G1GC (Garbage-First Garbage Collector).
    * CMS (-XX:+UseConcMarkSweepGC) was removed in JDK 14 — using it will crash the JVM on startup.
    * Parallel GC is still available but not the default.
    *
    * G1GC divides the heap into equal-sized regions and collects the "garbage-first" regions
    * (regions with the most reclaimable space). It targets pause-time goals rather than throughput.
    *
    * Key differences from the old defaults:
    *   - Parallel GC: maximizes throughput, longer stop-the-world pauses
    *   - CMS: low-pause, but fragmentation issues and removed in JDK 14
    *   - G1GC: balanced — predictable pause times, good throughput, handles large heaps well
    */

  val spark = SparkSession.builder()
    .appName("G1GC Tuning")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  /*
    G1GC key tuning parameters for Spark executors:

    -XX:G1HeapRegionSize=<size>
      Size of each G1 region (1MB to 32MB, must be power of 2).
      Larger regions = fewer regions = less overhead for large heaps.
      Rule of thumb: aim for ~2048 regions. For a 32GB heap, use 16m.

    -XX:InitiatingHeapOccupancyPercent=<percent>  (default: 45)
      When to start concurrent marking. Lower = earlier GC, less risk of full GC.
      For Spark shuffle-heavy workloads with bursty allocation, try 35.

    -XX:G1ReservePercent=<percent>  (default: 10)
      Percentage of heap kept as free reserve to reduce promotion failures.
      Increase to 15-20 if you see "to-space exhausted" in GC logs.

    -XX:MaxGCPauseMillis=<ms>  (default: 200)
      Target max GC pause time. G1 will try (but not guarantee) to stay under this.
      For latency-sensitive streaming jobs, try 100. For batch, 200-500 is fine.

    -XX:ParallelGCThreads=<n>
      Number of threads for parallel phases. Default = number of cores (capped at 8 + 5/8 of cores above 8).
      On Spark executors with spark.executor.cores=4, this is fine at default.

    -XX:ConcGCThreads=<n>
      Threads for concurrent marking. Default = ParallelGCThreads / 4.
      Increase if concurrent marking can't keep up (check GC logs for "concurrent cycle" timing).
  */

  // ==================================================================================
  // MEASURING GC — read the JVM's own GC counters via the platform MXBeans.
  // No log parsing needed: every JVM exposes cumulative GC time + count per collector.
  // ==================================================================================

  case class GcSnapshot(totalCount: Long, totalTimeMs: Long, collectors: Seq[String]) {
    def -(other: GcSnapshot): GcSnapshot =
      GcSnapshot(totalCount - other.totalCount, totalTimeMs - other.totalTimeMs, collectors)
    def avgPauseMs: Double = if (totalCount == 0) 0.0 else totalTimeMs.toDouble / totalCount
  }

  def gcSnapshot(): GcSnapshot = {
    val beans = ManagementFactory.getGarbageCollectorMXBeans.asScala
    GcSnapshot(
      totalCount = beans.map(_.getCollectionCount).filter(_ >= 0).sum,
      totalTimeMs = beans.map(_.getCollectionTime).filter(_ >= 0).sum,
      collectors = beans.map(_.getName).toSeq
    )
  }

  /** The collector names reveal which GC is active:
    *   G1GC      -> "G1 Young Generation", "G1 Concurrent GC", "G1 Old Generation"
    *   ParallelGC-> "PS Scavenge", "PS MarkSweep"
    *   SerialGC  -> "Copy", "MarkSweepCompact"
    *   ZGC       -> "ZGC Cycles", "ZGC Pauses"
    */
  def activeCollector(): String = gcSnapshot().collectors.mkString(", ")

  // a workload that puts real pressure on the heap:
  //  - builds wide string payloads (lots of short-lived objects -> young-gen churn)
  //  - caches a large dataset in the JVM heap (long-lived objects -> old-gen pressure)
  //  - runs shuffle-heavy aggregations + a self-join (allocation bursts -> GC during shuffle)
  def runWorkload(rows: Long, iterations: Int): Unit = {
    val base = spark.range(0, rows)
      .selectExpr(
        "id",
        "id % 2000 as group_key",
        // a chunky payload string so each row allocates real bytes on the heap
        "concat(cast(id as string), '-', repeat('x', 64)) as payload"
      )
      .persist(StorageLevel.MEMORY_ONLY) // force long-lived objects into the heap

    base.count() // materialize the cache

    var sink = 0L
    for (_ <- 1 to iterations) {
      val grouped = base
        .groupBy("group_key")
        .count()

      // a self-join on the grouped result keeps the shuffle machinery busy
      val joined = grouped.as("a")
        .join(grouped.as("b"), "group_key")
        .selectExpr("group_key", "a.count + b.count as total")

      sink += joined.agg(org.apache.spark.sql.functions.sum("total")).first().getLong(0)
    }

    base.unpersist()
    println(s"  (workload checksum: $sink)")
  }

  def demonstrateGCImpact(rows: Long = 30000000L, iterations: Int = 5): Unit = {
    val heapMaxMb = Runtime.getRuntime.maxMemory() / (1024 * 1024)

    println("=" * 70)
    println(s"Active garbage collector: ${activeCollector()}")
    println(s"Max heap (-Xmx): ${heapMaxMb} MB")
    println(s"Workload: $rows rows, $iterations iterations")
    println("=" * 70)

    val before = gcSnapshot()
    val wallStart = System.nanoTime()

    runWorkload(rows, iterations)

    val wallMs = (System.nanoTime() - wallStart) / 1e6
    val delta = gcSnapshot() - before

    val gcPct = if (wallMs == 0) 0.0 else 100.0 * delta.totalTimeMs / wallMs
    println("-" * 70)
    println(f"Wall-clock time:        ${wallMs}%.0f ms")
    println(f"GC collections:         ${delta.totalCount}%d")
    println(f"Total GC pause time:    ${delta.totalTimeMs}%d ms")
    println(f"Average pause / GC:     ${delta.avgPauseMs}%.1f ms")
    println(f"GC overhead:            ${gcPct}%.1f%% of wall-clock time")
    println("-" * 70)
    if (gcPct > 10.0)
      println(">> GC overhead exceeds 10% of runtime — this workload would benefit from GC tuning.")
    else
      println(">> GC overhead is under the 10% rule-of-thumb threshold for this heap/collector.")
    println()
  }

  /*
    PROVING THE BENEFIT — run the SAME workload under different collectors.

    GC flags must be set at JVM launch, so you can't switch collectors inside one run.
    Use the comparison script, which launches this main three times with different flags:

      ./scripts/run-gc-comparison.sh        # defaults: 8M rows, 6 iters, 2g heap
      ./scripts/run-gc-comparison.sh 12000000 8 1500m

    What you'll observe (typical, on a constrained heap):
      - SerialGC:   fewest threads, longest total pauses, highest GC%
      - ParallelGC: high throughput but large individual pauses (high avg pause/GC)
      - G1GC:       many small pauses (low avg pause/GC), GC% bounded near MaxGCPauseMillis goal

    The teaching point is the AVERAGE PAUSE / GC column: G1 trades a few more collections
    for much shorter individual stop-the-world pauses — that's the "pause-time goal" in action.

    How to pass GC flags to Spark executors and driver in a real cluster:

    spark-submit \
      --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:InitiatingHeapOccupancyPercent=35 -Xlog:gc*:file=/tmp/executor-gc.log:time,uptime,level,tags" \
      --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:G1HeapRegionSize=16m -Xlog:gc*:file=/tmp/driver-gc.log" \
      ...

    JDK 17 GC logging uses the unified -Xlog format (not the old -XX:+PrintGCDetails):
      -Xlog:gc*:file=gc.log:time,uptime,level,tags
      -Xlog:gc+heap=debug   (for heap details)

    Monitor GC behavior in the Spark UI:
      - Executor tab → GC Time column
      - If GC Time > 10% of task time, GC tuning is needed
      - Check the executor logs for full GC pauses

    Common scenarios and recommendations:
      1. Large shuffle with many small objects → increase G1HeapRegionSize
      2. Frequent full GCs → lower InitiatingHeapOccupancyPercent
      3. Long GC pauses on large heaps → consider off-heap caching (StorageLevel.OFF_HEAP)
      4. Streaming with strict latency → lower MaxGCPauseMillis to 100ms
  */

  def main(args: Array[String]): Unit = {
    // optional args: rows iterations   (defaults are tuned for a ~1.5g heap)
    val rows = args.lift(0).map(_.toLong).getOrElse(30000000L)
    val iterations = args.lift(1).map(_.toInt).getOrElse(5)

    spark.sparkContext.setLogLevel("WARN")
    demonstrateGCImpact(rows, iterations)
    spark.stop()
  }
}
