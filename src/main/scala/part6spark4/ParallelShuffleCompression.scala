package part6spark4

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.util.concurrent.atomic.AtomicLong

object ParallelShuffleCompression {

  /**
    * UPDATE - New lesson for Spark 4.
    *
    * Spark 4 introduced parallel shuffle compression for ZSTD and LZF codecs.
    * Previously, shuffle compression was single-threaded, making it a bottleneck
    * on multi-core executors with shuffle-heavy workloads.
    *
    * IMPORTANT: spark.io.compression.codec is a CORE SparkConf, read once when the
    * executor starts — NOT a runtime SQL conf. So spark.conf.set("spark.io.compression.codec", ...)
    * after the session is up has NO effect on the shuffle codec. To compare codecs honestly we
    * build a fresh SparkConf (and thus a fresh SparkContext) per codec, below.
    */

  /*
    Compression codecs available for shuffle (spark.io.compression.codec):

    | Codec  | Ratio  | Speed   | Parallel in Spark 4? | Best for                    |
    |--------|--------|---------|----------------------|-----------------------------|
    | lz4    | Low    | Fastest | No                   | CPU-bound, low-latency      |
    | snappy | Low    | Fast    | No                   | General purpose (old default)|
    | zstd   | High   | Medium  | YES (new!)           | I/O-bound, cloud storage    |
    | lzf    | Medium | Fast    | YES (new!)           | Balanced                    |

    Key configurations:
      spark.io.compression.codec = zstd            (default: lz4)
      spark.io.compression.zstd.workers = <n>      (0 = single-threaded; >0 = parallel threads, Spark 4)
      spark.io.compression.zstd.level = 1          (compression level, 1-22, default 1)
      spark.shuffle.compress = true                 (default: true)
      spark.shuffle.spill.compress = true           (default: true)
  */

  // A listener that totals the REAL shuffle bytes Spark wrote across all tasks — the same
  // number you'd read off the Spark UI "Shuffle Write" column, captured programmatically so
  // the demo can PROVE the compression ratio rather than assert it.
  class ShuffleWriteListener extends SparkListener {
    private val bytesWritten = new AtomicLong(0)
    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      val m = taskEnd.taskMetrics
      if (m != null && m.shuffleWriteMetrics != null)
        bytesWritten.addAndGet(m.shuffleWriteMetrics.bytesWritten)
    }
    def totalBytes: Long = bytesWritten.get()
  }

  case class Result(codec: String, zstdWorkers: Int, timeMs: Long, shuffleBytes: Long) {
    def shuffleMb: Double = shuffleBytes.toDouble / (1024 * 1024)
  }

  // Build a fresh context with the codec baked into the SparkConf, run a single big shuffle,
  // and report wall-clock time + actual shuffle-write bytes.
  def benchmark(codec: String, zstdWorkers: Int = 0, master: String = "local[*]",
                rows: Long = 15000000L, zstdLevel: Int = 3, shufflePartitions: Int = 64): Result = {
    val conf = new SparkConf()
      .setAppName(s"Shuffle Compression [$codec workers=$zstdWorkers]")
      .setMaster(master)
      .set("spark.io.compression.codec", codec)
      .set("spark.shuffle.compress", "true")
      .set("spark.sql.shuffle.partitions", shufflePartitions.toString)
      .set("spark.sql.adaptive.enabled", "false") // keep partition count fixed for a fair size comparison
    if (codec == "zstd") {
      conf.set("spark.io.compression.zstd.workers", zstdWorkers.toString)
      conf.set("spark.io.compression.zstd.level", zstdLevel.toString)
    }

    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val listener = new ShuffleWriteListener
    spark.sparkContext.addSparkListener(listener)

    try {
      // realistic, log-like payload: lots of repeated literal text (compressible) mixed with
      // variable ids (entropy) — exactly the kind of data shuffled in real ETL jobs.
      val df = spark.range(0, rows)
        .select(
          (col("id") % 5000).as("key"),
          concat(
            lit("2026-06-19T10:00:00 level=INFO service=checkout user_id="), col("id").cast("string"),
            lit(" session="), (col("id") % 100000).cast("string"),
            lit(" path=/api/v1/orders/items status=200 region=us-east-1 latency_ms="),
            (col("id") % 800).cast("string")
          ).as("payload")
        )

      val t0 = System.nanoTime()
      // repartition by key shuffles the FULL rows (payload included) -> one big shuffle write.
      // noop sink forces execution without writing output, isolating the shuffle cost.
      df.repartition(shufflePartitions, col("key"))
        .write.format("noop").mode("overwrite").save()
      val timeMs = ((System.nanoTime() - t0) / 1e6).toLong

      Result(codec, zstdWorkers, timeMs, listener.totalBytes)
    } finally {
      spark.stop()
    }
  }

  def main(args: Array[String]): Unit = {
    ratioDemo()
    // The parallel-compression comparison is opt-in because it deliberately uses a high zstd
    // level to make compression CPU-bound, which is slow. Run it with:
    //   runMain part6spark4.ParallelShuffleCompression parallel
    if (args.contains("parallel")) parallelDemo()
  }

  // --- Part 1: compression ratio across codecs (proves smaller shuffle write) ---
  def ratioDemo(): Unit = {
    // warm up the JVM/JIT so the first measured codec isn't penalised by cold start
    println("Warming up...")
    benchmark("lz4", rows = 3000000L)

    val codecRuns = Seq(
      benchmark("lz4"),
      benchmark("snappy"),
      benchmark("zstd", zstdWorkers = 0)
    )

    println()
    println("=" * 78)
    println("SHUFFLE COMPRESSION — same job, same data, different codec")
    println("=" * 78)
    println(f"${"codec"}%-10s ${"shuffle write"}%16s ${"vs lz4"}%10s ${"time"}%10s")
    val lz4Bytes = codecRuns.head.shuffleBytes.toDouble
    codecRuns.foreach { r =>
      val ratio = 100.0 * r.shuffleBytes / lz4Bytes
      println(f"${r.codec}%-10s ${r.shuffleMb}%13.1f MB ${ratio}%9.1f%% ${r.timeMs}%8d ms")
    }
    println(">> Smaller shuffle write = less network transfer between executors and less disk I/O.")
    println(">> ZSTD trades CPU for a markedly smaller shuffle — the win grows on I/O-bound clusters.")
  }

  // --- Part 2: parallel ZSTD (the Spark 4 headline feature) ---
  // HONEST CAVEAT: parallel compression only helps when there are SPARE cores for the worker
  // threads. On a fully-saturated local[*] every core is already running a task, so we cap task
  // parallelism (local[2]) to leave headroom AND crank the zstd level so compression is the
  // bottleneck — mirroring a real executor where single-threaded ZSTD throttles a big shuffle.
  // On a busy single laptop you may still see only a modest gain; the effect is a cluster-scale
  // one. The shuffle SIZE is identical either way — workers change SPEED, not ratio.
  def parallelDemo(): Unit = {
    println()
    println("=" * 78)
    println("PARALLEL ZSTD — single-threaded (workers=0) vs parallel (workers=4), master=local[2]")
    println("high zstd level + few large shuffle streams so compression CPU is the bottleneck")
    println("=" * 78)
    val rows = 8000000L
    val level = 12
    val parts = 4
    val zstdSerial = benchmark("zstd", zstdWorkers = 0, master = "local[2]", rows = rows, zstdLevel = level, shufflePartitions = parts)
    val zstdParallel = benchmark("zstd", zstdWorkers = 4, master = "local[2]", rows = rows, zstdLevel = level, shufflePartitions = parts)
    println(f"zstd workers=0 (Spark 3 behaviour): ${zstdSerial.timeMs}%6d ms, ${zstdSerial.shuffleMb}%.1f MB")
    println(f"zstd workers=4 (Spark 4 parallel):  ${zstdParallel.timeMs}%6d ms, ${zstdParallel.shuffleMb}%.1f MB")
    val speedup = zstdSerial.timeMs.toDouble / math.max(1, zstdParallel.timeMs)
    println(f">> Parallel compression speedup on this workload: ${speedup}%.2fx")
    println("   (Same shuffle size — workers change SPEED, not ratio. The gain scales with")
    println("    executor cores and shuffle volume; it is largest on fat executors.)")

    /*
      Enabling parallel ZSTD compression in production:

      spark-submit \
        --conf spark.io.compression.codec=zstd \
        --conf spark.io.compression.zstd.workers=4 \
        --conf spark.io.compression.zstd.level=1 \
        ...

      The 'workers' parameter controls how many threads ZSTD uses per compression stream.
      Setting it > 0 enables multi-threaded compression. Good starting point: match it to
      spark.executor.cores or half of it.

      When to use ZSTD over LZ4:
        - Reading/writing from cloud storage (S3, GCS, ADLS) where I/O is the bottleneck
        - Large shuffle sizes where 30%+ compression improvement saves significant network transfer
        - Multi-core executors where parallel compression threads are available

      When to stick with LZ4:
        - CPU-constrained workloads
        - Small shuffle sizes where compression overhead dominates
        - Single-core executors where parallel compression has no benefit
    */
  }
}
