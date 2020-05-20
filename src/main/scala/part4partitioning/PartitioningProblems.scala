package part4partitioning

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator

object PartitioningProblems {

  val spark = SparkSession.builder()
    .appName("Partitioning Problems")
    .master("local[*]") // for parallelism
    .getOrCreate()

  def processNumbers(nPartitions: Int) = {
    val numbers = spark.range(100000000) // ~800MB
    val repartitionedNumbers = numbers.repartition(nPartitions)
    repartitionedNumbers.cache()
    repartitionedNumbers.count()

    // the computation I care about
    repartitionedNumbers.selectExpr("sum(id)").show()
  }

  // 1 - use size estimator
  def dfSizeEstimator() = {
    val numbers = spark.range(100000)
    println(SizeEstimator.estimate(numbers)) // usually works, not super accurate, within an order of magnitude - larger number
    // measures the memory footprint of the actual JVM object backing the dataset
    numbers.cache()
    numbers.count()
  }

  // 2 - use query plan
  def estimateWithQueryPlan() = {
    val numbers = spark.range(100000)
    println(numbers.queryExecution.optimizedPlan.stats.sizeInBytes) // accurate size in bytes for the DATA
  }

  def estimateRDD() = {
    val numbers = spark.sparkContext.parallelize(1 to 100000)
    numbers.cache().count()
  }

  def main(args: Array[String]): Unit = {
//    processNumbers(2) // 400MB / partition
//    processNumbers(20) // 40MB / partition
//    processNumbers(200) // 4MB / partition
//    processNumbers(2000) // 400KB / partition
//    processNumbers(20000) // 40KB / partition

    // dfSizeEstimator()
    // estimateWithQueryPlan()
    // estimateRDD()

    // 10-100MB rule for partition size for UNCOMPRESSED DATA
    Thread.sleep(10000000)
  }

}
