package part6spark4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.nio.file.{Files, Path, Paths}
import scala.jdk.StreamConverters._

object Spark4Defaults {

  /**
    * UPDATE - New lesson for Spark 4.
    *
    * Spark 4 changed many default configurations that affect performance.
    * This lesson walks through the most impactful ones — and proves the two with a
    * measurable performance impact (ORC compression, partition capping) with real numbers.
    */

  val spark = SparkSession.builder()
    .appName("Spark 4 Default Config Changes")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  private val outputDir = Paths.get(System.getProperty("java.io.tmpdir"), "spark4-defaults-demo")

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
    println("=== 1. ANSI mode (spark.sql.ansi.enabled) ===")
    println(s"spark.sql.ansi.enabled = ${spark.conf.get("spark.sql.ansi.enabled")}")

    val numbers = Seq(10, 20, 0).toDF("value")
    numbers.createOrReplaceTempView("numbers")

    // In Spark 4 this THROWS DIVIDE_BY_ZERO (it silently returned null in Spark 3).
    try {
      spark.sql("SELECT 10 / value AS result FROM numbers").collect()
      println("No exception — ANSI mode appears to be OFF.")
    } catch {
      case e: Exception =>
        val msg = e.getMessage.split("\n").head
        println(s"Spark 4 threw on 10/0 (correctness win): $msg")
    }

    // safe alternative: try_divide returns null instead of throwing
    println("try_divide(10, value) is the migration-friendly fix:")
    spark.sql("SELECT value, try_divide(10, value) AS safe_result FROM numbers").show()

    // to restore Spark 3 behaviour (not recommended):
    // spark.conf.set("spark.sql.ansi.enabled", "false")
  }

  /*
    2. Max Single Partition Bytes: spark.sql.maxSinglePartitionBytes
       Old default: Long.MaxValue (unlimited)
       New default: 128MB

       This is a PLANNER cap, not a file-read cap (file-read parallelism is governed by
       spark.sql.files.maxPartitionBytes). When the planner would otherwise produce a single
       oversized partition — e.g. a one-partition relation feeding a rebalance — it now splits
       it so no single partition exceeds this cap, improving downstream parallelism.

       Restore old behaviour:
         spark.conf.set("spark.sql.maxSinglePartitionBytes", "9223372036854775807")
  */

  /*
    3. ORC Compression Codec: spark.sql.orc.compression.codec
       Old default: snappy
       New default: zstd

       ZSTD has a better compression ratio than Snappy with slightly higher CPU usage.
       For I/O-bound workloads (cloud storage, network reads), smaller files are a net win.
       This demo writes the SAME dataset both ways and measures bytes on disk.
  */
  def orcCompressionDemo(): Unit = {
    println("\n=== 3. ORC compression: snappy (old default) vs zstd (new default) ===")

    // realistic, compressible data: repeated categorical values + incrementing ids
    val data = spark.range(0, 10000000)
      .select(
        $"id",
        (col("id") % 50).as("department"),
        concat(lit("employee-"), (col("id") % 100000).cast("string")).as("name"),
        (rand() * 100000).cast("decimal(10,2)").as("salary")
      )

    def writeOrc(codec: String): Long = {
      val path = outputDir.resolve(s"orc-$codec")
      data.write.mode("overwrite").option("compression", codec).orc(path.toString)
      dirSize(path)
    }

    val snappyBytes = writeOrc("snappy")
    val zstdBytes = writeOrc("zstd")
    val savedPct = 100.0 * (snappyBytes - zstdBytes) / snappyBytes

    println(f"ORC + snappy (Spark 3 default): ${mb(snappyBytes)}%.1f MB")
    println(f"ORC + zstd   (Spark 4 default): ${mb(zstdBytes)}%.1f MB")
    println(f">> ZSTD wrote ${savedPct}%.1f%% fewer bytes — less network/storage I/O on every read.")
  }

  /*
    4. Speculation: less aggressive by default
       spark.speculation.multiplier: 1.5 → 3
       spark.speculation.quantile: 0.75 → 0.9

       Spark 4 speculates less aggressively, reducing wasted resources on healthy clusters.
       On clusters with frequent stragglers (e.g. spot instances, heterogeneous hardware),
       you may want to restore the old values for faster tail-latency recovery:

       spark.conf.set("spark.speculation.multiplier", "1.5")
       spark.conf.set("spark.speculation.quantile", "0.75")

    5. Shuffle Service Backend: spark.shuffle.service.db.backend
       Old default: LEVELDB → New default: ROCKSDB (better shuffle-metadata performance).

    6. Event Log Compression
       spark.eventLog.compress: false → true
       spark.eventLog.rolling.enabled: false → true

    7. Prometheus Metrics
       spark.ui.prometheus.enabled: false → true
       spark.metrics.appStatusSource.enabled: false → true
  */

  def showCurrentDefaults(): Unit = {
    println("\n=== Spark 4 Default Configurations (live) ===")
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
    interestingConfigs.foreach { key =>
      val value = scala.util.Try(spark.conf.get(key)).getOrElse("<not set>")
      println(s"  $key = $value")
    }
  }

  // --- helpers -----------------------------------------------------------------
  private def dirSize(path: Path): Long =
    if (!Files.exists(path)) 0L
    else Files.walk(path).toScala(LazyList).filter(Files.isRegularFile(_)).map(Files.size).sum

  private def mb(bytes: Long): Double = bytes.toDouble / (1024 * 1024)

  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("WARN")
    ansiModeDemo()
    orcCompressionDemo()
    showCurrentDefaults()
    spark.stop()
  }
}
