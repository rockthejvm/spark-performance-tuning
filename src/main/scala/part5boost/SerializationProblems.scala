package part5boost

import org.apache.spark.sql.SparkSession

object SerializationProblems {

  val spark = SparkSession.builder()
    .appName("Serialization Problems")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  val rdd = sc.parallelize(1 to 100)

  class RDDMultiplier {
    def multiplyRDD() = rdd.map(_ * 2).collect().toList
  }
  val rddMultiplier = new RDDMultiplier
  // rddMultiplier.multiplyRDD()
  // works

  // make class serializable
  class MoreGeneralRDDMultiplier extends Serializable {
    val factor = 2
    def multiplyRDD() = rdd.map(_ * factor).collect().toList
  }
  val moreGeneralRDDMultiplier = new MoreGeneralRDDMultiplier
  // moreGeneralRDDMultiplier.multiplyRDD()

  // technique: enclose member in local value
  class MoreGeneralRDDMultiplierEnclosure {
    val factor = 2
    def multiplyRDD() = {
      val enclosedFactor = factor
      rdd.map(_ * enclosedFactor).collect().toList
    }
  }
  val moreGeneralRDDMultiplier2 = new MoreGeneralRDDMultiplierEnclosure
  // moreGeneralRDDMultiplier2.multiplyRDD()

  /**
    * Exercise
    */
  class MoreGeneralRDDMultiplierNestedClass {
    val factor = 2

    object NestedMultiplier extends Serializable {
      val extraTerm = 10
      val localFactor = factor

      def multiplyRDD() = rdd.map(_ * localFactor + extraTerm).collect().toList
    }
  }
  val nestedMultiplier = new MoreGeneralRDDMultiplierNestedClass
  // nestedMultiplier.NestedMultiplier.multiplyRDD()

  /**
    * Exercise 2
    */
  case class Person(name: String, age: Int)
  val people = sc.parallelize(List(
    Person("Alice", 43),
    Person("Bob", 12),
    Person("Charlie", 23),
    Person("Diana", 67)
  ))

  class LegalDrinkingAgeChecker(legalAge: Int) {
    def processPeople() = {
      val ageThreshold = legalAge // capture the constructor argument in a local value
      people.map(_.age >= ageThreshold).collect().toList
    }
  }
  val peopleChecker = new LegalDrinkingAgeChecker(21)
  // peopleChecker.processPeople()

  def main(args: Array[String]): Unit = {

  }
}
