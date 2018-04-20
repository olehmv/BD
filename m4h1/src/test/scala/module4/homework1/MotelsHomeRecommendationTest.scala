package module4.homework1

import java.io.File
import java.nio.file.Files

import module4.homework1.MotelsHomeRecommendation.{AGGREGATED_DIR, ERRONEOUS_DIR}
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext, SparkContextProvider}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.junit._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

/**
  * Created by Csaba_Bejan on 8/17/2016.
  */
class MotelsHomeRecommendationTest extends FunSuite with SharedSparkContext with RDDComparisons with BeforeAndAfter
  with BeforeAndAfterAll with SparkContextProvider {

  override def conf = new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test").set("spark.testing.memory", "2147480000").set("spark.executor.heartbeatInterval","30")

  val INPUT_BIDS_SAMPLE = "m4h1/src/test/resources/bids_sample.txt"
  val INPUT_EXCHANGE_RATE = "m4h1/src/test/resources/integration/input/exchange_rates_small"
  val INPUT_BIDS_INTEGRATION = "m4h1/src/test/resources/integration/input/bids.txt"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "m4h1/src/test/resources/integration/input/exchange_rate.txt"
  val INPUT_MOTELS_INTEGRATION = "m4h1/src/test/resources/integration/input/motels.txt"

  val EXPECTED_AGGREGATED_INTEGRATION = "m4h1/src/test/resources/integration/expected_output/aggregated"
  val EXPECTED_ERRORS_INTEGRATION = "m4h1/src/test/resources/integration/expected_output/error_records"

  private var outputFolder: File = null

  before {
    outputFolder = Files.createTempDirectory("output").toFile
  }

  test("should read raw bids") {
    val expected = sc.parallelize(
      Seq(
        List("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", "", "1.35"),
        List("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL")
      )
    )

    val rawBids = MotelsHomeRecommendation.getRawBids(sc, INPUT_BIDS_SAMPLE)

    assertRDDEquals(expected, rawBids)
  }

  test("should collect erroneous records") {
    val rawBids = sc.parallelize(
      Seq(
        List("1", "06-05-02-2016", "ERROR_1"),
        List("2", "15-04-08-2016", "0.89"),
        List("3", "07-05-02-2016", "ERROR_2"),
        List("4", "06-05-02-2016", "ERROR_1"),
        List("5", "06-05-02-2016", "ERROR_2")
      )
    )

    val expected = sc.parallelize(
      Seq(
        "06-05-02-2016,ERROR_1,2",
        "06-05-02-2016,ERROR_2,1",
        "07-05-02-2016,ERROR_2,1"
      )
    )

    val erroneousRecords = MotelsHomeRecommendation.getErroneousRecords(rawBids)

    assertRDDEquals(expected, erroneousRecords)
  }




  test("should read motels") {
    val expectedOutput = sc.parallelize(
      Seq(
        ("0000001", "Olinda Windsor Inn"),
        ("0000002", "Merlin Por Motel"),
        ("0000003", "Olinda Big River Casino"),
        ("0000004", "Majestic Big River Elegance Plaza"),
        ("0000005", "Majestic Ibiza Por Hostel")
      )
    )
    val motels = MotelsHomeRecommendation.getMotels(sc, INPUT_MOTELS_INTEGRATION)
    assertRDDEquals(expectedOutput, sc.parallelize(motels.take(5)))
  }


  test("should load exchange rates") {
    var stringToDouble: Map[String, Double] = MotelsHomeRecommendation.getExchangeRates(sc, INPUT_EXCHANGE_RATE)
    val exchangeRates = stringToDouble
    val expected: Map[String, Double] = sc.parallelize(
      Seq(
        ("11-06-05-2016", 0.803),
        ("11-05-08-2016",  0.873),
        ("10-06-11-2015", 0.987),
        ("10-05-02-2016",  0.876)
      )
    ).collect().toMap
    assert(exchangeRates, expected)
  }



  test("should expand bids") {
    //5, 6, 8
    val rawBids = sc.parallelize(
      Seq(
        List("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", "1.32", "1.32", "1.35"),
        List("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL"),
        List("0000003", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", "", "", "")
      )
    )
    val exchangeRate = sc.parallelize(
      Seq(
        ("15-04-08-2016", 0.803),
        ("06-05-02-2016",  0.873)
      )
    ).collect().toMap

    val expected = sc.parallelize(
      Seq(
        BidItem("0000002", "2016-08-04 15:00", "US", 1.662),
        BidItem("0000002", "2016-08-04 15:00", "CA", 1.084),
        BidItem("0000002", "2016-08-04 15:00", "MX", 1.06),
        BidItem("0000003", "2016-08-04 15:00", "US", 1.662)
      )
    )
    val result = MotelsHomeRecommendation.getBids(rawBids, exchangeRate)
    assertRDDEquals(expected, result)
  }


  test("should enrich with motel names") {
    val input = sc.parallelize(Seq(
      BidItem("0000002", "2016-08-04 15:00", "US", 1.662),
      BidItem("0000002", "2016-08-04 15:00", "CA", 1.084),
      BidItem("0000002", "2016-08-04 15:00", "MX", 1.06),
      BidItem("0000003", "2016-08-04 15:00", "US", 1.662),
      BidItem("0000011", "2016-08-04 15:00", "US", 1.662)
    ))
    val motels = sc.parallelize(
      Seq(
        ("0000001", "Olinda Windsor Inn"),
        ("0000002", "Merlin Por Motel"),
        ("0000003", "Olinda Big River Casino")
      ))
    val expected = sc.parallelize(
      Seq(
        EnrichedItem("0000002", "Merlin Por Motel", "2016-08-04 15:00", "US", 1.662),
        EnrichedItem("0000003", "Olinda Big River Casino", "2016-08-04 15:00", "US", 1.662)
      )
    )
    val result = MotelsHomeRecommendation.getEnriched(input, motels)
    val test = result.collect()
    assertRDDEquals(expected, result)
  }



  test("should filter errors and create correct aggregates") {

    runIntegrationTest()

    //If the test fails and you are interested in what are the differences in the RDDs uncomment the corresponding line
    //printRddDifferences(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    //printRddDifferences(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))

    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertAggregatedFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  after {
    outputFolder.delete
  }

  private def runIntegrationTest() = {
    MotelsHomeRecommendation.processData(sc, INPUT_BIDS_INTEGRATION, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def assertRddTextFiles(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    assertRDDEquals(expected, actual)
  }

  private def assertAggregatedFiles(expectedPath: String, actualPath: String) = {
    val expected = parseLastDouble(sc.textFile(expectedPath)).collect.toMap
    val actual = parseLastDouble(sc.textFile(actualPath)).collect.toMap
    if (expected.size != actual.size) {
      Assert.fail(s"Aggregated have wrong number of records (${actual.size} instead of ${expected.size})")
    }
    expected.foreach(x => {
      val key = x._1
      val expectedValue = x._2
      if (!actual.contains(key)) {
        Assert.fail(s"Aggregated does not contain: $key,$expectedValue")
      }
      val actualValue = actual(key)
      if (Math.abs(expectedValue - actualValue) > 0.0011) {
        Assert.fail(s"Aggregated have different value for: $key ($actualValue instead of $expectedValue)")
      }
    })
  }

  private def parseLastDouble(rdd: RDD[String]) = {
    rdd.map(s => {
      val commaIndex = s.lastIndexOf(",")
      (s.substring(0, commaIndex), s.substring(commaIndex + 1).toDouble)
    })
  }

  private def printRddDifferences(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    RddComparator.printDiff(expected, actual)
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }
}
