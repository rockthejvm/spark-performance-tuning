package common

import scala.util.Random

object DataGenerator {

  val random = new Random()

  /////////////////////////////////////////////////////////////////////////////////
  // General data generation
  /////////////////////////////////////////////////////////////////////////////////

  def randomDouble(limit: Double): Double = {
    assert(limit >= 0)
    random.nextDouble() * limit
  }

  def randomLong(limit: Long = Long.MaxValue): Long = {
    assert(limit >= 0)
    Math.abs(random.nextLong()) % limit
  }

  def randomInt(limit: Int = Int.MaxValue): Int = {
    assert(limit >= 0)
    random.nextInt(limit)
  }

  def randomIntBetween(low: Int, high: Int) = {
    assert(low <= high)
    random.nextInt(high - low) + low
  }

  def randomString(n: Int) = random.alphanumeric.take(n).mkString("")

  def pickFrom[T](seq: Seq[T]): T = {
    assert(seq.nonEmpty)
    seq(randomInt(seq.length))
  }


  /////////////////////////////////////////////////////////////////////////////////
  // Guitars generation - fixing skewed data lecture
  /////////////////////////////////////////////////////////////////////////////////

  val guitarModelSet: Seq[(String, String)] = Seq(
    ("Taylor", "914"),
    ("Martin", "D-18"),
    ("Takamine", "P7D"),
    ("Gibson", "L-00"),
    ("Tanglewood", "TW45"),
    ("Fender", "CD-280S"),
    ("Yamaha", "LJ16BC")
  )

  /**
    * Generates a tuple of (make, model) from the available set.
    * If 'uniform' is false, it will have a 50% chance of picking one pair, and 50% chance for all the others.
    */
  def randomGuitarModel(uniform: Boolean = false): (String, String) = {
    val makeModelIndex = if (!uniform && random.nextBoolean()) 0 else random.nextInt(guitarModelSet.size)
    guitarModelSet(makeModelIndex)
  }

  def randomSoundQuality() = s"4.${random.nextInt(9)}".toDouble
  def randomGuitarRegistration(): String = randomString(8)
  def randomGuitarModelType(): String = s"${randomString(4)}-${randomString(4)}"
  def randomGuitarPrice() = 500 + random.nextInt(1500)

  def randomGuitar(uniformDist: Boolean = false): Guitar = {
    val makeModel = randomGuitarModel(uniformDist)
    Guitar(randomGuitarModelType(), makeModel._1, makeModel._2, randomSoundQuality())
  }

  def randomGuitarSale(uniformDist: Boolean = false): GuitarSale = {
    val makeModel = randomGuitarModel(uniformDist)
    GuitarSale(randomGuitarRegistration(), makeModel._1, makeModel._2, randomSoundQuality(), randomGuitarPrice())
  }
}