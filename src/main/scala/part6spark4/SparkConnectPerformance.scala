package part6spark4

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SparkConnectPerformance {

  /**
    * UPDATE - New lesson for Spark 4.
    *
    * Spark Connect is the client-server architecture that decouples your application
    * from the Spark driver. Communication happens over gRPC (port 15002 by default).
    *
    * Flow:
    *   Client (your code)
    *     → builds unresolved logical plan as protobuf
    *     → sends via gRPC to Spark Connect server
    *     → server resolves plan with Catalyst, optimizes, executes
    *     → results stream back as Apache Arrow batches
    *
    * What DOESN'T change: once the plan reaches the server, the full Spark engine runs —
    * Catalyst, Tungsten, WholeStageCodegen, AQE, shuffle, caching, partitioning.
    * All techniques from the rest of this course apply identically.
    *
    * What DOES change: a new performance surface around plan analysis, schema access,
    * result transfer, and client resource usage.
    *
    * RDD API is NOT available in Spark Connect — DataFrame and SQL only.
    */

  /*
    Starting a Spark Connect session:

    Option A — embedded (for development, what we use below):
      SparkSession.builder()
        .appName("My App")
        .master("local[*]")
        .config("spark.api.mode", "connect")
        .getOrCreate()

    Option B — remote server:
      // first start the server: ./sbin/start-connect-server.sh
      SparkSession.builder()
        .remote("sc://localhost:15002")
        .getOrCreate()

    Option C — remote with auth:
      SparkSession.builder()
        .remote("sc://myhost:443/;use_ssl=true;token=ABCDEFG")
        .getOrCreate()
  */

  val spark = SparkSession.builder()
    .appName("Spark Connect Performance")
    .master("local[*]")
    .config("spark.api.mode", "connect")
    .getOrCreate()

  import spark.implicits._

  // ==================================================================================
  // 1. PLAN CACHING
  // ==================================================================================
  /*
    In Spark Connect, every action (collect, show, explain) sends the logical plan to the
    server for analysis. When you build DataFrames incrementally — common in notebooks and
    ETL pipelines — the same sub-plans get re-analyzed on every call.

    Plan caching (SPARK-47818) caches resolved logical plans on the server side.
    Benchmark: 200 iterative DataFrame constructions dropped from ~110s to ~5s (22x faster).

    Configs:
      spark.connect.session.planCache.enabled = true           (default, can toggle at runtime)
      spark.connect.session.planCache.maxSize = 32             (max cached plans per session)
      spark.connect.session.planCache.alwaysCacheDataSourceReadsEnabled = true (caches data source metadata)
  */

  def demonstratePlanCaching(): Unit = {
    val iterations = 100

    // build a chain of DataFrames iteratively (simulates notebook-style exploration)
    def buildIterativeDF(n: Int): DataFrame = {
      var df = spark.range(1000000).toDF("id")
      for (i <- 1 to n) {
        df = df.withColumn(s"col_$i", col("id") * i)
      }
      df
    }

    // with plan cache enabled (default)
    spark.conf.set("spark.connect.session.planCache.enabled", "true")
    val t0 = System.nanoTime()
    val dfCached = buildIterativeDF(iterations)
    dfCached.explain() // triggers plan analysis
    val cachedTime = (System.nanoTime() - t0) / 1e6

    // with plan cache disabled
    spark.conf.set("spark.connect.session.planCache.enabled", "false")
    val t1 = System.nanoTime()
    val dfUncached = buildIterativeDF(iterations)
    dfUncached.explain()
    val uncachedTime = (System.nanoTime() - t1) / 1e6

    println(s"Plan cache ON:  ${cachedTime.toLong}ms")
    println(s"Plan cache OFF: ${uncachedTime.toLong}ms")

    // re-enable
    spark.conf.set("spark.connect.session.planCache.enabled", "true")
  }

  // ==================================================================================
  // 2. SCHEMA RPC OVERHEAD
  // ==================================================================================
  /*
    In classic Spark, df.schema is an in-memory field — instant access.
    In Spark Connect, df.schema triggers an AnalyzePlan RPC to the server every time.

    Anti-pattern: accessing schema in a loop
    Fix: cache schema in a local val
  */

  def demonstrateSchemaOverhead(): Unit = {
    val df = spark.range(1000000)
      .withColumn("name", lit("test"))
      .withColumn("value", rand())

    // anti-pattern: schema access in a loop (N round-trips)
    val t0 = System.nanoTime()
    for (_ <- 1 to 50) {
      val _ = df.schema  // each call is an RPC!
      val _ = df.columns // also an RPC!
    }
    val slowTime = (System.nanoTime() - t0) / 1e6

    // fix: cache schema locally (1 round-trip)
    val t1 = System.nanoTime()
    val cachedSchema = df.schema
    val cachedColumns = df.columns
    for (_ <- 1 to 50) {
      val _ = cachedSchema
      val _ = cachedColumns
    }
    val fastTime = (System.nanoTime() - t1) / 1e6

    println(s"Schema in loop (50 RPCs): ${slowTime.toLong}ms")
    println(s"Cached schema (1 RPC):    ${fastTime.toLong}ms")
    println(s"Schema: $cachedSchema")
  }

  // ==================================================================================
  // 3. RESULT SET TRANSFER — ARROW SERIALIZATION
  // ==================================================================================
  /*
    Results in Spark Connect are ALWAYS serialized as Apache Arrow batches.
    (In classic PySpark, you had to enable this manually with spark.sql.execution.arrow.pyspark.enabled.)

    Arrow is columnar with zero-copy capability — up to 100x faster than row-based serialization.

    Key configs:
      spark.connect.grpc.maxInboundMessageSize = 128MB   (max single gRPC message)
      spark.connect.session.resultChunking.maxChunkSize   (~115MB, 90% of max message)
      spark.connect.grpc.arrow.maxBatchSize = 128MB       (max Arrow batch size)

    Rule of thumb: avoid collect() on large DataFrames. Use df.write, df.show(n),
    or df.limit(n).collect() instead. Every collected row must traverse the network.
  */

  def demonstrateCollectOverhead(): Unit = {
    // small collect — fast, no concern
    val t0 = System.nanoTime()
    val small = spark.range(1000).collect()
    val smallTime = (System.nanoTime() - t0) / 1e6

    // medium collect — noticeable serialization + transfer
    val t1 = System.nanoTime()
    val medium = spark.range(1000000).collect()
    val mediumTime = (System.nanoTime() - t1) / 1e6

    // large collect — significant overhead
    val t2 = System.nanoTime()
    val large = spark.range(10000000).collect()
    val largeTime = (System.nanoTime() - t2) / 1e6

    println(s"Collect 1K rows:  ${smallTime.toLong}ms")
    println(s"Collect 1M rows:  ${mediumTime.toLong}ms")
    println(s"Collect 10M rows: ${largeTime.toLong}ms")
    println("In classic Spark, small/medium collects are near-instant (in-process).")
    println("In Spark Connect, every row crosses the gRPC boundary.")

    // the right approach: push computation to the server, collect only the result
    val t3 = System.nanoTime()
    val result = spark.range(10000000).selectExpr("sum(id)").collect()
    val aggTime = (System.nanoTime() - t3) / 1e6
    println(s"Aggregate then collect 1 row: ${aggTime.toLong}ms (always prefer this)")
  }

  // ==================================================================================
  // 4. PLAN COMPRESSION (Spark 4.1+)
  // ==================================================================================
  /*
    Complex plans (many joins, unions, nested subqueries) can produce large protobuf payloads.
    Spark 4.1 added ZSTD compression for plans exceeding a size threshold.

    Configs:
      spark.connect.session.planCompression.threshold = 10MB     (compress plans larger than this)
      spark.connect.session.planCompression.defaultAlgorithm = ZSTD
      spark.connect.maxPlanSize = 512MB                          (max decompressed plan size)

    Set threshold to -1 to disable compression (if CPU-constrained).
    For very complex pipelines with 100+ transformations, this reduces gRPC payload significantly.
  */

  // ==================================================================================
  // 5. WHEN TO USE SPARK CONNECT vs CLASSIC
  // ==================================================================================
  /*
    Use Spark Connect when:
      - Multi-tenant: many users sharing one Spark cluster (notebooks, BI tools)
      - Resource-constrained clients: thin client uses ~200MB vs 1.5GB+ for embedded driver
      - Language diversity: Python, Scala, Java, Go, Rust, Swift clients
      - Server stability: client crash doesn't kill the Spark session
      - Fast iteration: no JVM cold start, server is always running

    Use Classic (embedded) when:
      - You need the RDD API
      - Ultra-low-latency: avoiding any gRPC overhead (rare — only matters for sub-second jobs)
      - You need SparkContext internals (custom accumulators, broadcast variables at RDD level)
      - Legacy libraries that depend on SparkContext

    In practice, most production workloads on managed platforms (Databricks, EMR, Dataproc)
    already use Spark Connect under the hood.
  */

  // ==================================================================================
  // 6. SPARK CONNECT CONFIGURATION REFERENCE
  // ==================================================================================
  /*
    Server-side configs (set at server startup via --conf):

    | Configuration                                          | Default    | Description                          |
    |--------------------------------------------------------|------------|--------------------------------------|
    | spark.connect.grpc.binding.port                        | 15002      | gRPC server port                     |
    | spark.connect.grpc.maxInboundMessageSize               | 128 MB     | Max inbound gRPC message             |
    | spark.connect.grpc.arrow.maxBatchSize                  | 128 MB     | Max Arrow batch size                 |
    | spark.connect.session.planCache.enabled                | true       | Plan cache toggle (runtime)          |
    | spark.connect.session.planCache.maxSize                | 32         | Plans cached per session (static)    |
    | spark.connect.session.planCompression.threshold        | 10 MB      | Compress plans above this size       |
    | spark.connect.session.planCompression.defaultAlgorithm | ZSTD       | Compression algorithm                |
    | spark.connect.session.manager.defaultSessionTimeout    | 60m        | Session idle timeout                 |
    | spark.connect.maxPlanSize                              | 512 MB     | Max decompressed plan size           |
    | spark.connect.execute.reattachable.enabled             | true       | Reattachable execution               |

    Client connection string parameters:
      sc://host:port/;token=X;use_ssl=true;user_id=X;session_id=X;grpc_max_message_size=X
  */

  def main(args: Array[String]): Unit = {
    demonstratePlanCaching()
    println("---")
    demonstrateSchemaOverhead()
    println("---")
    demonstrateCollectOverhead()
    Thread.sleep(1000000)
  }
}
