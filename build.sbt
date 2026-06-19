
ThisBuild / version := "0.3"

// UPDATE - Spark 4.1.1 requires Scala 2.13.17+ and JDK 17+
ThisBuild / scalaVersion := "2.13.17"

val sparkVersion = "4.1.1"
val log4jVersion = "2.24.3"

// Main course module: classic Spark (spark-sql / spark-core).
lazy val root = (project in file("."))
  .settings(
    name := "spark-performance-tuning",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      // logging
      "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
      "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
    )
  )

// Spark Connect client module (Part 6). It uses spark-connect-client-jvm, which provides
// its OWN org.apache.spark.sql.SparkSession and CANNOT share a classpath with spark-sql —
// hence a separate module. Run against the Connect server in spark4-cluster/docker-compose.yml:
//   sbt "connect/runMain part6spark4.SparkConnectPerformance"
lazy val connect = (project in file("connect"))
  .settings(
    name := "spark-connect-demo",
    libraryDependencies += "org.apache.spark" %% "spark-connect-client-jvm" % sparkVersion
  )
