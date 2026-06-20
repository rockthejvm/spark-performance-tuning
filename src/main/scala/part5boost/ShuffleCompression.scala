package part5boost

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.functions._

import java.util.concurrent.atomic.AtomicLong

object ShuffleCompression {

  /*
    Once you set a compression codec in the spark conf (before starting the spark session)
    YOU CANNOT CHANGE IT.

    Tradeoff
    - shuffle size - data transfer (IO-bound clusters)
    - compression time
    - cpu load
   */

  class ShuffleWriteListener extends SparkListener {
    private val bytesWritten = new AtomicLong(0)

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      val metrics = taskEnd.taskMetrics
      if (metrics != null && metrics.shuffleWriteMetrics != null) {
        val shuffleBytes = metrics.shuffleWriteMetrics.bytesWritten
        bytesWritten.addAndGet(shuffleBytes)
      }
    }

    def totalBytes: Long = bytesWritten.get()
  }

  case class Result(
                   codec: String,
                   timeMs: Long,
                   shuffleBytes: Long
                   ) {
    def shuffleMb: Double = shuffleBytes.toDouble / (1024 * 1024)
  }

  // compression codecs: lz4, snappy, zstd, lzf
  def benchmark(codec: String, rows: Long = 15000000, shufflePartitions: Int = 64, zstdWorkers: Int = 0, zstdLevel: Int = 3) = {
    val conf = new SparkConf()
      .setAppName("Shuffle compression")
      .set("spark.io.compression.codec", codec)
      .set("spark.shuffle.compress", "true")
      .setMaster(if (codec == "zstd" && zstdWorkers > 0) "local[2]" else "local[*]")

    if (codec == "zstd" && zstdWorkers > 0) {
      conf.set("spark.io.compression.zstd.workers", zstdWorkers.toString)
      conf.set("spark.io.compression.zstd.level", zstdLevel.toString)
    }

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val listener = new ShuffleWriteListener
    spark.sparkContext.addSparkListener(listener)
    spark.sparkContext.setLogLevel("WARN")

    // wide strings - log lines
    try {
      val df = spark.range(0, rows)
        .select(
          (col("id") % 5000).as("key"),
          concat(
            lit("26/06/20 15:39:18 INFO MyService"),
            lit("session="), (col("id") % 10000).cast("string"),
            lit(": Server created on 192.168.1.149:64414 path=/opt/spark/mything/app/... user="),
            (col("id") % 800).cast("string")
          ).as("payload")
        )

      val t0 = System.nanoTime()
      // force the evaluation of this DF
      df.repartition(shufflePartitions, col("key"))
        .write.format("noop").mode("overwrite").save()
      val finish = System.nanoTime()
      val timeMs = ((finish - t0) / 1e6).toLong
      Result(codec, timeMs, shuffleBytes = listener.totalBytes)
    } finally {
      spark.stop()
    }
  }

  def runBenchmark() = {
    // warmup
    benchmark("lz4", rows = 3000000)

    val codecs = List("lz4", "snappy", "zstd")
    val parallelRun = benchmark("zstd", zstdWorkers = 4, zstdLevel = 12)
    val codecRuns = codecs.map(codec => benchmark(codec)) :+ parallelRun
    codecRuns.foreach(println)
  }

  def main(args: Array[String]): Unit = {
    runBenchmark()
  }
}
