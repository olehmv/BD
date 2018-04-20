package homework5

import java.io.File
import java.nio.file.Files

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons, SharedSparkContext, SparkContextProvider}
import homework5.MotelsHomeRecommendation.{AGGREGATED_DIR, ERRONEOUS_DIR}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

class MotelsHomeRecommendationTest extends FunSuite with SharedSparkContext with RDDComparisons with DataFrameSuiteBase with BeforeAndAfter
  with BeforeAndAfterAll with SparkContextProvider{
  override def conf = new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test").set(" hive.exec.scratchdir", "D:\\tmp\\hive").set("spark.testing.memory", "2147480000").set("spark.executor.heartbeatInterval","30")


  val INPUT_BIDS_SAMPLE = "src/test/resources/bids_sample.txt"

  val INPUT_BIDS_INTEGRATION = "m5h1/src/test/resources/integration/input/bids.gz.parquet"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "m5h1/src/test/resources/integration/input/exchange_rate.txt"
  val INPUT_MOTELS_INTEGRATION = "m5h1/src/test/resources/integration/input/motels.gz.parquet"

  val EXPECTED_AGGREGATED_INTEGRATION = "m5h1/src/test/resources/integration/expected_output/aggregated"
  val EXPECTED_ERRORS_INTEGRATION = "m5h1/src/test/resources/integration/expected_output/expected_sql"

  private var outputFolder: File = null
  var hiveContext: SQLContext= null
  before {
    outputFolder =Files.createTempDirectory("output").toFile
    hiveContext = SparkSession
      .builder()
      .appName("MaxBidSearch")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate().sqlContext
  }

  test("should filter errors and create correct aggregates"){

    runIntegrationTest()

    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertRddTextFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  test("should load the motels") {
    val expectedOutput: DataFrame = sqlContext.createDataFrame(sc.parallelize(
      Seq(
        Row("0000001", "Olinda Windsor Inn", null, null, null),
        Row("0000002", "Merlin Por Motel", null, null, null),
        Row("0000003", "Olinda Big River Casino", null, null, null)
      )), StructType(Array("MotelID", "MotelName", "Country", "URL", "Comment")
      .map(field => StructField(field, StringType, true))))
    val result = MotelsHomeRecommendation.getMotels(sqlContext, INPUT_MOTELS_INTEGRATION)
    assertDataFrameEquals(expectedOutput.select("MotelID", "motelName"), result.select("MotelID", "motelName").limit(3))
  }

  test("should convert date") {
    val context = new HiveContext(sc)
    import context.implicits._
    val actual = Seq(("12-07-06-2015")).toDF("original_date")
    val expected = Seq(("2015-06-07 12:00")).toDF("converted_date")
    val result = actual.select(MotelsHomeRecommendation.getConvertDate($"original_date")).alias("converted_date").toDF("converted_date")
    assertDataFrameEquals(expected, result)
  }
  test("should load exchange rates") {
    val expected = hiveContext.createDataFrame(sc.parallelize(
      Seq(
        Row("11-06-05-2016","Euro", "EUR", "0.803"),
        Row("11-05-08-2016", "Euro", "EUR", "0.873"),
        Row("10-06-11-2015", "Euro", "EUR", "0.987"),
        Row("10-05-02-2016", "Euro", "EUR", "0.876")
      )),  StructType(Array("ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate")
      .map(field => StructField(field, StringType, true))))
    val result = MotelsHomeRecommendation.getExchangeRates(hiveContext, INPUT_EXCHANGE_RATES_INTEGRATION)
    assertDataFrameEquals(expected, result.limit(4))
  }

  test("should aggregate result") {
    val context = hiveContext
    import context.implicits._
    val input = hiveContext.createDataset(sc.parallelize(
      Seq(
        BidItem("0000003", "15-04-09-2016", "US", 1.50),
        BidItem("0000003", "16-04-09-2016", "US", 1.50),
        BidItem("0000002", "15-04-08-2016", "US", 1.30),
        BidItem("0000002", "15-04-08-2016", "CA", 1.10),
        BidItem("0000002", "15-04-08-2016", "MX", 1.09)
      ))
    ).toDF
    val expected = hiveContext.createDataset(sc.parallelize(
      Seq(
        BidItem("0000003", "16-04-09-2016", "US", 1.50),
        BidItem("0000003", "15-04-09-2016", "US", 1.50),
        BidItem("0000002", "15-04-08-2016", "US", 1.30)
      ))
    ).toDF
    val actual = MotelsHomeRecommendation.getAggregatedBids(input)
    assertDataFrameEquals(expected, actual)
  }
  after{
    outputFolder.delete
  }

  private def runIntegrationTest() = {
    MotelsHomeRecommendation.processData(hiveContext, INPUT_BIDS_INTEGRATION, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def assertRddTextFiles(expectedPath: String, actualPath: String) = {
    val expected =  sc.textFile(expectedPath)
    val actual  = sc.textFile(actualPath)
    RddComparator.printDiff(expected, actual)
    assertRDDEquals(expected, actual)
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }
}

//object MotelsHomeRecommendationTest {
//  var sc: SparkContext = null
//  var sqlContext: HiveContext = null
//
//  @BeforeClass
//  def beforeTests() = {
//   sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test").set("spark.testing.memory", "2147480000").set("spark.executor.heartbeatInterval","30"))
//    sqlContext = new HiveContext(sc)
//  }
//
//  @AfterClass
//  def afterTests() = {
//    sc.stop
//  }
//}
