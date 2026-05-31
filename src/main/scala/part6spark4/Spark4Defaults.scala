package part6spark4

import org.apache.spark.sql.SparkSession

object Spark4Defaults {

  /**
    * UPDATE - New lesson for Spark 4.
    *
    * Spark 4 changed many default configurations that affect performance.
    * This lesson walks through the most impactful ones.
    */

  val spark = SparkSession.builder()
    .appName("Spark 4 Default Config Changes")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  /*
    1. ANSI Mode: spark.sql.ansi.enabled
       Old default: false
       New default: TRUE

       Queries that silently returned wrong results now throw exceptions:
         - Division by zero → DIVIDE_BY_ZERO (was null)
         - Overflow in casts → CAST_OVERFLOW (was silent wrapping)
         - Invalid array index → INVALID_ARRAY_INDEX (was null)

       This is a correctness improvement, but it WILL break existing queries
       that relied on silent null/overflow behavior.
  */
  def ansiModeDemo(): Unit = {
    val numbers = Seq(10, 20, 0).toDF("value")
    numbers.createOrReplaceTempView("numbers")

    // this will THROW in Spark 4 (DIVIDE_BY_ZERO), was null in Spark 3
    // spark.sql("SELECT 10 / value FROM numbers").show()

    // safe alternative: use try_divide
    spark.sql("SELECT try_divide(10, value) FROM numbers").show()

    // to restore Spark 3 behavior (not recommended):
    // spark.conf.set("spark.sql.ansi.enabled", "false")
  }

  /*
    2. Max Single Partition Bytes: spark.sql.maxSinglePartitionBytes
       Old default: Long.MaxValue (unlimited)
       New default: 128MB

       Partitions that previously could grow unbounded are now capped.
       This changes parallelism, shuffle behavior, and memory usage for jobs
       that relied on very large partitions.

       Restore old behavior:
         spark.conf.set("spark.sql.maxSinglePartitionBytes", "9223372036854775807")
  */

  /*
    3. ORC Compression Codec: spark.sql.orc.compression.codec
       Old default: snappy
       New default: zstd

       ZSTD has a ~30% better compression ratio than Snappy with slightly higher CPU usage.
       For I/O-bound workloads (cloud storage, network reads), ZSTD is a net win.
       For CPU-bound workloads, consider switching back to snappy or lz4.

       spark.conf.set("spark.sql.orc.compression.codec", "snappy") // restore old default
  */

  /*
    4. Speculation: less aggressive by default
       spark.speculation.multiplier: 1.5 → 3
       spark.speculation.quantile: 0.75 → 0.9

       Spark 4 speculates less aggressively, reducing wasted resources on healthy clusters.
       On clusters with frequent stragglers (e.g. spot instances, heterogeneous hardware),
       you may want to restore the old values for faster tail-latency recovery:

       spark.conf.set("spark.speculation.multiplier", "1.5")
       spark.conf.set("spark.speculation.quantile", "0.75")
  */

  /*
    5. Shuffle Service Backend: spark.shuffle.service.db.backend
       Old default: LEVELDB
       New default: ROCKSDB

       RocksDB offers better performance for shuffle metadata storage.
       This is transparent to most applications but worth knowing for debugging.
  */

  /*
    6. Event Log Compression
       spark.eventLog.compress: false → true
       spark.eventLog.rolling.enabled: false → true

       Event logs are now compressed and rolled by default, reducing storage on the history server.
  */

  /*
    7. Prometheus Metrics
       spark.ui.prometheus.enabled: false → true
       spark.metrics.appStatusSource.enabled: false → true

       Monitoring endpoints are now on by default — useful for Grafana/Prometheus dashboards.
  */

  def showCurrentDefaults(): Unit = {
    val interestingConfigs = Seq(
      "spark.sql.ansi.enabled",
      "spark.sql.maxSinglePartitionBytes",
      "spark.sql.orc.compression.codec",
      "spark.speculation.multiplier",
      "spark.speculation.quantile",
      "spark.shuffle.service.db.backend",
      "spark.eventLog.compress",
      "spark.ui.prometheus.enabled"
    )

    println("=== Spark 4 Default Configurations ===")
    interestingConfigs.foreach { key =>
      val value = scala.util.Try(spark.conf.get(key)).getOrElse("<not set>")
      println(s"  $key = $value")
    }
  }

  def main(args: Array[String]): Unit = {
    ansiModeDemo()
    showCurrentDefaults()
  }
}
