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
    * This object lives in its OWN sbt module ("connect") because it depends on
    * spark-connect-client-jvm, which ships its own org.apache.spark.sql.SparkSession and
    * CANNOT share a classpath with spark-sql. That is the whole point of Connect: the client
    * is a thin gRPC client, not an embedded Spark driver.
    *
    * Flow:
    *   Client (this code)
    *     → builds unresolved logical plan as protobuf
    *     → sends via gRPC to the Spark Connect server
    *     → server resolves plan with Catalyst, optimizes, executes
    *     → results stream back as Apache Arrow batches
    *
    * What DOESN'T change: once the plan reaches the server, the full Spark engine runs —
    * Catalyst, Tungsten, WholeStageCodegen, AQE, shuffle, caching, partitioning.
    * All techniques from the rest of this course apply identically.
    *
    * What DOES change: a new performance surface around plan analysis, schema access,
    * result transfer, and client resource usage — which is what we measure below.
    *
    * RDD API is NOT available in Spark Connect — DataFrame and SQL only.
    *
    * ── HOW TO RUN ──────────────────────────────────────────────────────────────────
    *   1. Start the Connect server (see spark4-cluster/README.md):
    *        cd spark4-cluster && docker compose up -d spark-connect
    *   2. Run this client (defaults to sc://localhost:15002, override with SPARK_REMOTE):
    *        sbt "connect/runMain part6spark4.SparkConnectPerformance"
    */

  // The client connects to a REMOTE server — there is no local master here.
  val remote: String = sys.env.getOrElse("SPARK_REMOTE", "sc://localhost:15002")

  val spark: SparkSession = SparkSession.builder()
    .remote(remote)
    .getOrCreate()

  import spark.implicits._

  // ==================================================================================
  // 1. PLAN CACHING  (SPARK-47818) — the biggest Connect-specific win
  // ==================================================================================
  /*
    In Spark Connect, every action (collect, show, explain) sends the logical plan to the
    server for analysis. When you build DataFrames incrementally — common in notebooks and
    ETL pipelines — the same sub-plans get re-analyzed on every call.

    Plan caching caches resolved logical plans on the server side, so re-analysis is skipped.

    Configs (runtime-settable from the client; they're forwarded to the server):
      spark.connect.session.planCache.enabled = true   (default)
      spark.connect.session.planCache.maxSize = 32      (max cached plans per session)
  */
  def demonstratePlanCaching(iterations: Int = 200): Unit = {
    def buildIterativeDF(n: Int): DataFrame = {
      var df = spark.range(1000000).toDF("id")
      for (i <- 1 to n) df = df.withColumn(s"col_$i", col("id") * i)
      df
    }

    // cache OFF: every analyze re-walks the whole growing plan tree
    spark.conf.set("spark.connect.session.planCache.enabled", "false")
    warm()
    val t0 = System.nanoTime()
    buildIterativeDF(iterations).schema // forces server-side analysis
    val offMs = (System.nanoTime() - t0) / 1e6

    // cache ON: resolved sub-plans are reused
    spark.conf.set("spark.connect.session.planCache.enabled", "true")
    warm()
    val t1 = System.nanoTime()
    buildIterativeDF(iterations).schema
    val onMs = (System.nanoTime() - t1) / 1e6

    println("=" * 70)
    println(s"PLAN CACHING — $iterations chained withColumn() transformations")
    println("=" * 70)
    println(f"Plan cache OFF: ${offMs}%.0f ms")
    println(f"Plan cache ON:  ${onMs}%.0f ms")
    println(f">> Speedup: ${offMs / math.max(1.0, onMs)}%.1fx — re-analysis of the plan tree is skipped.")
    println()
  }

  // ==================================================================================
  // 2. SCHEMA RPC OVERHEAD
  // ==================================================================================
  /*
    In classic Spark, df.schema is an in-memory field — instant. In Spark Connect, df.schema
    triggers an AnalyzePlan RPC to the server EVERY time. Same for df.columns, df.dtypes.

    Anti-pattern: accessing schema in a loop -> N round-trips.
    Fix: cache it in a local val -> 1 round-trip.
  */
  def demonstrateSchemaOverhead(loops: Int = 50): Unit = {
    val df = spark.range(1000000).withColumn("name", lit("test")).withColumn("value", rand())
    warm()

    val t0 = System.nanoTime()
    for (_ <- 1 to loops) { df.schema; df.columns } // each is an RPC!
    val slowMs = (System.nanoTime() - t0) / 1e6

    val t1 = System.nanoTime()
    val cachedSchema = df.schema
    val cachedColumns = df.columns
    for (_ <- 1 to loops) { cachedSchema; cachedColumns } // local, no RPC
    val fastMs = (System.nanoTime() - t1) / 1e6

    println("=" * 70)
    println(s"SCHEMA ACCESS — $loops iterations")
    println("=" * 70)
    println(f"schema/columns in loop (${2 * loops} RPCs): ${slowMs}%.0f ms")
    println(f"cached locally (2 RPCs):                  ${fastMs}%.0f ms")
    println(f">> Speedup: ${slowMs / math.max(1.0, fastMs)}%.1fx — cache schema, don't re-fetch it.")
    println()
  }

  // ==================================================================================
  // 3. RESULT SET TRANSFER — ARROW SERIALIZATION
  // ==================================================================================
  /*
    Results in Spark Connect are ALWAYS serialized as Apache Arrow batches and streamed over
    gRPC. collect() on a large DataFrame must move every row across the network. The fix is
    almost always: push the computation to the server and collect only the small result.
  */
  def demonstrateCollectOverhead(): Unit = {
    warm()
    def timed[T](b: => T): Double = { val t = System.nanoTime(); b; (System.nanoTime() - t) / 1e6 }

    val small = timed(spark.range(1000).collect())
    val medium = timed(spark.range(1000000).collect())
    val large = timed(spark.range(10000000).collect())
    val aggregated = timed(spark.range(10000000).selectExpr("sum(id)").collect())

    println("=" * 70)
    println("RESULT TRANSFER — collect() must stream every row over gRPC")
    println("=" * 70)
    println(f"collect 1K rows:                 ${small}%.0f ms")
    println(f"collect 1M rows:                 ${medium}%.0f ms")
    println(f"collect 10M rows:                ${large}%.0f ms")
    println(f"aggregate server-side, collect 1 row: ${aggregated}%.0f ms")
    println(">> Aggregating on the server and collecting one row is the right pattern —")
    println("   it avoids dragging millions of rows across the gRPC boundary.")
    println()
  }

  // ==================================================================================
  // 4. PLAN COMPRESSION (Spark 4.1+) / 5. WHEN TO USE CONNECT / 6. CONFIG REFERENCE
  // ==================================================================================
  /*
    Plan compression: complex plans (many joins/unions/subqueries) produce large protobuf
    payloads. Spark 4.1 ZSTD-compresses plans over a threshold:
      spark.connect.session.planCompression.threshold = 10MB
      spark.connect.session.planCompression.defaultAlgorithm = ZSTD
      spark.connect.maxPlanSize = 512MB

    Use Connect when: multi-tenant notebooks/BI, thin clients (~200MB vs 1.5GB driver),
    polyglot clients (Python/Scala/Go/Rust), client-crash isolation, fast iteration.
    Use Classic when: you need the RDD API or SparkContext internals.

    Server-side config reference:
    | Configuration                                   | Default | Description               |
    |-------------------------------------------------|---------|---------------------------|
    | spark.connect.grpc.binding.port                 | 15002   | gRPC server port          |
    | spark.connect.grpc.maxInboundMessageSize        | 128 MB  | Max inbound gRPC message  |
    | spark.connect.grpc.arrow.maxBatchSize           | 128 MB  | Max Arrow batch size      |
    | spark.connect.session.planCache.enabled         | true    | Plan cache toggle         |
    | spark.connect.session.planCache.maxSize         | 32      | Plans cached per session  |
    | spark.connect.session.planCompression.threshold | 10 MB   | Compress plans over this  |
    | spark.connect.maxPlanSize                        | 512 MB  | Max decompressed plan     |

    Client connection string parameters:
      sc://host:port/;token=X;use_ssl=true;user_id=X;session_id=X;grpc_max_message_size=X
  */

  // tiny no-op to pay one-time connection/JIT warmup before a measured block
  private def warm(): Unit = spark.range(1).collect()

  def main(args: Array[String]): Unit = {
    println(s"Connected to Spark Connect server at: $remote")
    println(s"Server Spark version: ${spark.version}\n")

    demonstratePlanCaching()
    demonstrateSchemaOverhead()
    demonstrateCollectOverhead()

    spark.stop()
  }
}
