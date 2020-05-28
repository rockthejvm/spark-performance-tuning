package part5boost

import common._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object FixingDataSkews {

  val spark = SparkSession.builder()
    .appName("Fixing Data Skews")
    .master("local[*]")
    .getOrCreate()

  // deactivate broadcast joins
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  import spark.implicits._

  val guitars: Dataset[Guitar] = Seq.fill(40000)(DataGenerator.randomGuitar()).toDS
  val guitarSales: Dataset[GuitarSale] = Seq.fill(200000)(DataGenerator.randomGuitarSale()).toDS

  /*
    A Guitar is similar to a GuitarSale if
    - same make and model
    - abs(guitar.soundScore - guitarSale.soundScore) <= 0.1

    Problem:
    - for every Guitar, avg(sale prices of ALL SIMILAR GuitarSales)
    - Gibson L-00, config "sadfhja", sound 4.3,
      compute avg(sale prices of ALL GuitarSales of Gibson L-00 with sound quality between 4.2 and 4.4
   */

  def naiveSolution() = {
    val joined = guitars.join(guitarSales, Seq("make", "model"))
      .where(abs(guitarSales("soundScore") - guitars("soundScore")) <= 0.1)
      .groupBy("configurationId")
      .agg(avg("salePrice").as("averagePrice"))

    joined.explain()
    joined.count()
  }

  def noSkewSolution() = {
    // salting interval 0-99
    val explodedGuitars = guitars.withColumn("salt", explode(lit((0 to 99).toArray))) // multiplying the guitars DS x100
    val saltedGuitarSales = guitarSales.withColumn("salt", monotonically_increasing_id() % 100)

    val nonSkewedJoin = explodedGuitars.join(saltedGuitarSales, Seq("make", "model", "salt"))
      .where(abs(saltedGuitarSales("soundScore") - explodedGuitars("soundScore")) <= 0.1)
      .groupBy("configurationId")
      .agg(avg("salePrice").as("averagePrice"))

    nonSkewedJoin.explain()
    nonSkewedJoin.count()
  }

  def main(args: Array[String]): Unit = {
    noSkewSolution()
    Thread.sleep(1000000)
  }
}
