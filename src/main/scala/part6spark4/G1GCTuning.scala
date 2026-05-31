package part6spark4

import org.apache.spark.sql.SparkSession

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

  def demonstrateGCImpact(): Unit = {
    // a shuffle-heavy workload that generates GC pressure
    val numbers = spark.range(0, 50000000)
    val grouped = numbers
      .selectExpr("id", "id % 1000 as group_key", "cast(id as string) as payload")
      .groupBy("group_key")
      .count()

    grouped.explain()
    grouped.show()
  }

  /*
    How to pass GC flags to Spark executors and driver:

    spark-submit \
      --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:InitiatingHeapOccupancyPercent=35 -XX:+PrintGCDetails -Xlog:gc*:file=/tmp/executor-gc.log" \
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
    demonstrateGCImpact()
    Thread.sleep(1000000)
  }
}
