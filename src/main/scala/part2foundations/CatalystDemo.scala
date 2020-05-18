package part2foundations

import org.apache.spark.sql.{DataFrame, SparkSession}

object CatalystDemo {

  val spark = SparkSession.builder()
    .appName("Catalyst Demo")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  val flights = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/flights")

  // Catalyst plays nice with chained filters
  val notFromHere = flights
    .where($"origin" =!= "LGA")
    .where($"origin" =!= "ORD")
    .where($"origin" =!= "SFO")
    .where($"origin" =!= "DEN")
    .where($"origin" =!= "BOS")
    .where($"origin" =!= "EWR")

  notFromHere.explain(true)

  // sometimes we do something redundant, out of ignorance or lack of communication with the rest of our team
  def filterTeam1(flights: DataFrame) = flights.where($"origin" =!= "LGA").where($"dest" === "DEN")
  def filterTeam2(flights: DataFrame) = flights.where($"origin" =!= "EWR").where($"dest" === "DEN")

  val filterBoth = filterTeam1(filterTeam2(flights))
  filterBoth.explain(true)

  // pushing down filters all the way to the data source - do not read records in the first place
  flights.write.save("src/main/resources/data/flights_parquet")

  val notFromLGA = spark.read.load("src/main/resources/data/flights_parquet")
    .where($"origin" =!= "LGA")

  notFromLGA.explain

  def main(args: Array[String]): Unit = {
    // empty
  }
}
