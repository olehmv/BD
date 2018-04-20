
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
val common= "org.apache.hadoop" % "hadoop-common" % "2.6.0" % "provided"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client
val client= "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "provided"
// https://mvnrepository.com/artifact/junit/junit
val junit= "junit" % "junit" % "4.11" % "test"
// https://mvnrepository.com/artifact/org.apache.mrunit/mrunit
val mrunit= "org.apache.mrunit" % "mrunit" % "1.1.0" % Test classifier "hadoop2" 
// https://mvnrepository.com/artifact/eu.bitwalker/UserAgentUtils
val userAgent= "eu.bitwalker" % "UserAgentUtils" % "1.14"
// https://mvnrepository.com/artifact/org.apache.hive/hive-service
val hive= "org.apache.hive" % "hive-service" % "2.3.2" % "provided" excludeAll
                       ExclusionRule (organization = "org.pentaho") 
val sparkCore= "org.apache.spark" %% "spark-core" % "2.2.0" % "provided"
// https://mvnrepository.com/artifact/com.holdenkarau/spark-testing-base
val sparkTest= "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.7.4" % Test
val scalaDateTime= "com.github.nscala-time" %% "nscala-time" % "2.12.0"
// https://mvnrepository.com/artifact/com.databricks/spark-csv
val sparkCSV="com.databricks" %% "spark-csv" % "1.5.0" % "provided"
val sparkHive="org.apache.spark" %% "spark-hive" % "2.2.0" % "provided"

// https://mvnrepository.com/artifact/org.numenta/htm.java
val htmJava= "org.numenta" % "htm.java" % "0.6.13" excludeAll
                       ExclusionRule (organization = "algorithmfoundry") 

// https://mvnrepository.com/artifact/com.opencsv/opencsv
val opencsv="com.opencsv" % "opencsv" % "3.10"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
val sparkSql= "org.apache.spark" %% "spark-sql" % "2.2.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
val sparkMlib= "org.apache.spark" %% "spark-mllib" % "2.2.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
val sparkStreaming= "org.apache.spark" %% "spark-streaming" % "2.2.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
val sparkStreamingKafka= "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0"


// https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-core
val jacksonCore= "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7"

// https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-databind
val jacksonDatabind="com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"

// https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-annotations
val jacksonAnnotation= "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.7"




lazy val commonSettings = Seq(
organization := "com.course.epam",
version := "1",
scalaVersion :="2.11.11",
//sbt update-classifiers, sbt eclipse -> this command download javadoc for eclipse project
EclipseKeys.withSource := true,
scalacOptions := Seq("-target:jvm-1.8"),
// adding the tools.jar to the unmanaged-jars seq
unmanagedJars in Compile ~= {uj =>
    Seq(Attributed.blank(file(System.getProperty("java.home").dropRight(3)+"lib/tools.jar"))) ++ uj
},
assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {_.data.getName == "scala-library-2.11.11.jar"}
}
)



lazy val m1h1 = (project in file("m1h1"))
    .settings(
    commonSettings,
    libraryDependencies += common,
    libraryDependencies += client,
    libraryDependencies += junit,
    libraryDependencies += mrunit
  )

lazy val m1h2 = (project in file("m1h2"))
   .settings(
    commonSettings,
     libraryDependencies += common,
    libraryDependencies += client,
    libraryDependencies += junit,
    libraryDependencies += mrunit,
    libraryDependencies += userAgent

   )

lazy val m1h3 = (project in file("m1h3"))
   .settings(
    commonSettings, 
    libraryDependencies += common,
    libraryDependencies += client,
    libraryDependencies += junit,
    libraryDependencies += mrunit,
    libraryDependencies += userAgent

   )
lazy val m2h3 = (project in file("m2h3"))
   .settings(
    commonSettings,
    libraryDependencies += common,
    libraryDependencies += client,
    libraryDependencies += junit,
    libraryDependencies += mrunit,
    libraryDependencies += userAgent,
    libraryDependencies += hive
   )
lazy val m4h1 = (project in file("m4h1"))
   .settings(
    commonSettings,
     libraryDependencies += common,
     libraryDependencies += client,
    libraryDependencies += sparkCore,
    libraryDependencies += sparkTest,
    libraryDependencies += junit,
    libraryDependencies += scalaDateTime
   )

lazy val m5h1 = (project in file("m5h1"))
  .settings(
    commonSettings,
    libraryDependencies += sparkCore,
    libraryDependencies += sparkTest,
    libraryDependencies += scalaDateTime,
    libraryDependencies += sparkCSV,
    libraryDependencies +=sparkHive

  )

lazy val m6h1 = (project in file("m6h1"))
  .settings(
    commonSettings,
    libraryDependencies += htmJava,
    libraryDependencies +=opencsv,
    libraryDependencies +=sparkSql,
    libraryDependencies +=sparkMlib,
    libraryDependencies +=sparkStreaming,
     libraryDependencies +=sparkStreamingKafka,
    libraryDependencies +=jacksonCore,
    libraryDependencies +=jacksonDatabind,
    libraryDependencies +=jacksonAnnotation

  )
