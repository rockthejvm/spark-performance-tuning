package part3caching

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Checkpointing {

  val spark = SparkSession.builder()
    .appName("Checkpointing")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext

  def demoCheckpoint() = {
    val flightsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/flights")

    // do some expensive computation
    val orderedFlights = flightsDF.orderBy("dist")

    // checkpointing is used to avoid failure in computations
    // needs to be configured
    sc.setCheckpointDir("spark-warehouse")

    // checkpoint a DF = save the DF to disk
    val checkpointedFlights = orderedFlights.checkpoint() // an action

    // query plan difference with checkpointed DFs
    /*
      == Physical Plan ==
      *(1) Sort [dist#16 ASC NULLS FIRST], true, 0
      +- *(1) Project [_id#7, arrdelay#8, carrier#9, crsarrtime#10L, crsdephour#11L, crsdeptime#12L, crselapsedtime#13, depdelay#14, dest#15, dist#16, dofW#17L, origin#18]
         +- BatchScan[_id#7, arrdelay#8, carrier#9, crsarrtime#10L, crsdephour#11L, crsdeptime#12L, crselapsedtime#13, depdelay#14, dest#15, dist#16, dofW#17L, origin#18] JsonScan Location: InMemoryFileIndex[file:/Users/daniel/dev/rockthejvm/courses/spark-optimization-2/src/main/resourc..., ReadSchema: struct<_id:string,arrdelay:double,carrier:string,crsarrtime:bigint,crsdephour:bigint,crsdeptime:b...
     */
    orderedFlights.explain()
    /*
      == Physical Plan ==
      *(1) Scan ExistingRDD[_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18]
     */
    checkpointedFlights.explain()

    checkpointedFlights.show()
  }

  def cachingJobRDD() = {
    val numbers = sc.parallelize(1 to 10000000)
    val descNumbers = numbers.sortBy(-_).persist(StorageLevel.DISK_ONLY)
    descNumbers.sum()
    descNumbers.sum() // shorter time here
  }

  def checkpointingJobRDD() = {
    sc.setCheckpointDir("spark-warehouse")
    val numbers = sc.parallelize(1 to 10000000)
    val descNumbers = numbers.sortBy(-_)
    descNumbers.checkpoint() // returns Unit
    descNumbers.sum()
    descNumbers.sum()
  }

  def cachingJobDF() = {
    val flightsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/flights")

    val orderedFlights = flightsDF.orderBy("dist")
    orderedFlights.persist(StorageLevel.DISK_ONLY)
    orderedFlights.count()
    orderedFlights.count() // shorter job
  }

  def checkpointingJobDF() = {
    sc.setCheckpointDir("spark-warehouse")
    val flightsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/flights")

    val orderedFlights = flightsDF.orderBy("dist")
    val checkpointedFlights = orderedFlights.checkpoint()
    checkpointedFlights.count()
    checkpointedFlights.count()
  }

  def main(args: Array[String]): Unit = {
    cachingJobDF()
    checkpointingJobDF()
    Thread.sleep(1000000)
  }
}
