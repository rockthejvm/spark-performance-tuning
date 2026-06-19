package part6spark4

import org.apache.spark.sql.SparkSession

object CpuThrottlingBenchmark {

  /**
    * UPDATE - New lesson for Spark 4 (Kubernetes).
    *
    * A deliberately CPU-BOUND Spark job used to prove the silent-CPU-throttling pitfall on
    * Kubernetes (see spark4-cluster/k8s/README.md). There is no shuffle and no real I/O — each
    * task just spins in a tight floating-point loop, so the ONLY thing that limits throughput
    * is how many CPU cycles the executor pod is allowed to use.
    *
    * Run it twice on K8s with the SAME spark.executor.cores but different CPU limits:
    *   - limit.cores == request.cores (tight)  -> the executor's task threads are throttled by
    *     the CFS quota and the job runs several times slower, with NO error or log line.
    *   - limit.cores >  request.cores (burstable) -> the threads burst onto spare cores and the
    *     job runs at full speed.
    *
    * The wall-clock difference, plus /sys/fs/cgroup/cpu.stat (nr_throttled), is the proof.
    *
    * Args: [rowsMillions] [innerIterations]   (defaults: 8 million ids, 20000 iters each)
    */

  def main(args: Array[String]): Unit = {
    val rows = args.lift(0).map(_.toLong).getOrElse(8L) * 1000000L
    val iters = args.lift(1).map(_.toInt).getOrElse(20000)

    val spark = SparkSession.builder()
      .appName("CPU Throttling Benchmark")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val cores = spark.conf.getOption("spark.executor.cores").getOrElse("?")
    // one partition per id-block; plenty of partitions so every core stays busy
    val partitions = 64

    println("=" * 64)
    println(s"CPU-bound benchmark: $rows ids x $iters iters, $partitions partitions")
    println(s"spark.executor.cores = $cores")
    println("=" * 64)

    val t0 = System.nanoTime()
    val checksum = spark.sparkContext
      .range(0, rows, 1, partitions)
      .map { id =>
        // a tight, branch-free FP loop — pure CPU work, no allocation, no I/O
        var x = id.toDouble + 1.0
        var k = 0
        while (k < iters) {
          x = math.sqrt(x * 1.0000001 + 1.0) + math.sin(x)
          k += 1
        }
        x
      }
      .sum()
    val wallMs = (System.nanoTime() - t0) / 1e6

    val rowsPerSec = rows / (wallMs / 1000.0)
    println("-" * 64)
    println(f"Wall-clock time:    ${wallMs}%.0f ms")
    println(f"Throughput:         ${rowsPerSec}%,.0f ids/sec")
    println(f"(checksum: ${checksum}%.3f)")
    println("-" * 64)
    println("Compare this wall-clock between the throttled and unthrottled submits.")
    println("Then read cpu.stat in the pod: kubectl exec <pod> -- cat /sys/fs/cgroup/cpu.stat")

    spark.stop()
  }
}
