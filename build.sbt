
name := "spark-performance-tuning"

version := "0.3"

// UPDATE - Spark 4.1.1 requires Scala 2.13.17+ and JDK 17+
scalaVersion := "2.13.17"
val sparkVersion = "4.1.1"
val log4jVersion = "2.24.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
)