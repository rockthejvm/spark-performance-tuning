package part6spark4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ParallelShuffleCompression {

  /**
    * UPDATE - New lesson for Spark 4.
    *
    * Spark 4 introduced parallel shuffle compression for ZSTD and LZF codecs.
    * Previously, shuffle compression was single-threaded, making it a bottleneck
    * on multi-core executors with shuffle-heavy workloads.
    */

  val spark = SparkSession.builder()
    .appName("Parallel Shuffle Compression")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

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
      spark.io.compression.zstd.workers = <n>      (number of parallel compression threads)
      spark.io.compression.zstd.level = 1          (compression level, 1-22, default 1)
      spark.shuffle.compress = true                 (default: true)
      spark.shuffle.spill.compress = true           (default: true)
  */

  def generateShuffleData() = {
    spark.range(0, 20000000)
      .selectExpr(
        "id",
        "id % 5000 as department",
        "cast(rand() * 100000 as decimal(10,2)) as salary",
        "cast(id as string) as name"
      )
  }

  def benchmarkCodec(codec: String): Unit = {
    spark.conf.set("spark.io.compression.codec", codec)

    val employees = generateShuffleData()

    // a shuffle-heavy aggregation
    val result = employees
      .groupBy("department")
      .agg(
        avg("salary").as("avg_salary"),
        count("*").as("count"),
        sum("salary").as("total_salary")
      )
      .orderBy($"avg_salary".desc)

    val start = System.currentTimeMillis()
    result.collect()
    val elapsed = System.currentTimeMillis() - start

    println(s"Codec: $codec — time: ${elapsed}ms")
    // check Spark UI → Stages → Shuffle Write / Shuffle Read for data sizes
  }

  /*
    Enabling parallel ZSTD compression:

    spark-submit \
      --conf spark.io.compression.codec=zstd \
      --conf spark.io.compression.zstd.workers=4 \
      --conf spark.io.compression.zstd.level=1 \
      ...

    The 'workers' parameter controls how many threads ZSTD uses per compression stream.
    Setting it > 0 enables multi-threaded compression.
    Good starting point: match it to spark.executor.cores or half of it.

    When to use ZSTD over LZ4:
      - Reading/writing from cloud storage (S3, GCS, ADLS) where I/O is the bottleneck
      - Large shuffle sizes where 30%+ compression improvement saves significant network transfer
      - Multi-core executors where parallel compression threads are available

    When to stick with LZ4:
      - CPU-constrained workloads
      - Small shuffle sizes where compression overhead dominates
      - Single-core executors where parallel compression has no benefit
  */

  def main(args: Array[String]): Unit = {
    benchmarkCodec("lz4")
    benchmarkCodec("snappy")
    benchmarkCodec("zstd")
    Thread.sleep(1000000)
  }
}
