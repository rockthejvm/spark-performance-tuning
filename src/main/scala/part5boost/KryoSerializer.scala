package part5boost

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object KryoSerializer {

  // 1 - define a SparkConf object with the Kryo serializer
  val sparkConf = new SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrationRequired", "true")
    .registerKryoClasses(Array( // 2 - register the classes we want to serialize
      classOf[Person],
      classOf[Array[Person]]
    ))

  val spark = SparkSession.builder()
    .appName("Kryo Serialization")
    .config(sparkConf) // 3 - pass the SparkConf object to the SparkSession
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  case class Person(name: String, age: Int)
  def generatePeople(nPersons: Int) = (1 to nPersons).map(i => Person(s"Person$i", i % 100))

  val people = sc.parallelize(generatePeople(10000000))

  def testCaching() = {
    people.persist(StorageLevel.MEMORY_ONLY_SER).count()
    /*
      Java serialization
      - memory usage 254MB
      - time 20s

      Kryo serialization
      - memory usage 164.5 MB
      - time 17s
     */
  }

  def testShuffling() = {
    people.map(p => (p.age, p)).groupByKey().mapValues(_.size).count()
    /*
      Java serialization
      - shuffle 72.5 MB
      - time 20s

      Kryo serialization
      - shuffle 42.8 MB
      - time 18s
     */
  }

  def main(args: Array[String]): Unit = {
    testShuffling()
    Thread.sleep(10000000)
  }


}
