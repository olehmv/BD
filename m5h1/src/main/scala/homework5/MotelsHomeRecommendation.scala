package homework5

import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation"))
    val sqlContext = new SQLContext(sc)

    processData(sqlContext, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sqlContext: SQLContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: DataFrame = getRawBids(sqlContext, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      */
    val erroneousRecords: DataFrame = getErroneousRecords(rawBids)
    erroneousRecords.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: DataFrame = getExchangeRates(sqlContext, exchangeRatesPath)

    /**
      * Task 3:
      * UserDefinedFunction to convert between date formats.
      * Hint: Check the formats defined in Constants class
      */
    val convertDate: UserDefinedFunction = getConvertDate

    /**
      * Task 3:
      * Transform the rawBids
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: DataFrame = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: DataFrame = getMotels(sqlContext, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names.
      */
    val enriched: DataFrame = getEnriched(bids, motels)
    enriched.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sqlContext: SQLContext, bidsPath: String): DataFrame = {
    var rawBids: DataFrame = sqlContext.sparkSession.read.load(bidsPath)
    var rows: Array[Row] = rawBids.collect()
    rawBids
  }

  def getErroneousRecords(rawBids: DataFrame): DataFrame = {
    var errorBids: DataFrame = rawBids.filter(error => error.getAs[String](2).startsWith("ERROR")).select("MotelID", "BidDate", "HU").toDF
    errorBids.groupBy("BidDate","HU").count().alias("count")
  }

  def getExchangeRates(sqlContext: SQLContext, exchangeRatesPath: String): DataFrame = {
    sqlContext.read.format(Constants.CSV_FORMAT).schema( StructType(Array("ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate")
      .map(field => StructField(field, StringType, true)))).load(exchangeRatesPath)
  }

  def getConvertDate: UserDefinedFunction = {
    val convertUDF: String => String = Constants.INPUT_DATE_FORMAT.parseDateTime(_).toString(Constants.OUTPUT_DATE_FORMAT)
    var function: UserDefinedFunction = udf(convertUDF)
    function
  }

  def getConvertCurrency: UserDefinedFunction = {
    val convertUFD = (value: String, exchangeRate: String) => {
      var maybeDouble: Option[Double] = toDouble(value)
      var double: Double = maybeDouble.getOrElse(0)
      BigDecimal(double * exchangeRate.toDouble).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
    }
    var function: UserDefinedFunction = udf(convertUFD)
    function
  }
  def toDouble(s: String): Option[Double] = {
    try {
      Some(s.toDouble)
    } catch {
      case e: NumberFormatException => None
      case e: NullPointerException => None
    }
  }


  def getBids(rawBids: DataFrame, exchangeRates: DataFrame): DataFrame = {
    var bids: DataFrame = rawBids.filter(error => (!error.getAs[String](2).startsWith("ERROR")))
      .join(exchangeRates, rawBids.col("BidDate") === exchangeRates("ValidFrom"))
    var targetBids: DataFrame = bids.select(bids.col("MotelId"), getConvertDate(bids.col("BidDate")).alias("BidDate")
      , getConvertCurrency(bids.col(Constants.TARGET_LOSAS(0)), bids.col("ExchangeRate")).alias(Constants.TARGET_LOSAS(0))
      , getConvertCurrency(bids.col(Constants.TARGET_LOSAS(1)), bids.col("ExchangeRate")).alias(Constants.TARGET_LOSAS(1))
      , getConvertCurrency(bids.col(Constants.TARGET_LOSAS(2)), bids.col("ExchangeRate")).alias(Constants.TARGET_LOSAS(2)))
  import rawBids.sqlContext.implicits._
    var result: DataFrame =targetBids.flatMap{
     row=>
       Seq(BidItem(row.getString(0), row.getString(1), Constants.TARGET_LOSAS(0), row.getDouble(2)),
         BidItem(row.getString(0), row.getString(1), Constants.TARGET_LOSAS(1), row.getDouble(3)),
         BidItem(row.getString(0), row.getString(1), Constants.TARGET_LOSAS(2), row.getDouble(4))
       )
   }.where("price > 0").toDF()
    result
  }

  def getMotels(sqlContext: SQLContext, motelsPath: String): DataFrame = {
    val motels = sqlContext.sparkSession.read.load(motelsPath)
    motels
  }

  def getEnriched(bids: DataFrame, motels: DataFrame): DataFrame = {
    var aggregatedBids: DataFrame = getAggregatedBids(bids)
    var join: DataFrame = aggregatedBids.join(motels, "MotelId")
    val result = join.select(aggregatedBids("MotelId"),motels("MotelName"),aggregatedBids("BidDate"),aggregatedBids("loSa"),aggregatedBids("price"))
    result
  }

  def getAggregatedBids(bids: DataFrame): DataFrame = {
    var price: WindowSpec = Window.partitionBy(bids.col("motelID"), bids.col("bidDate")).orderBy(desc("price"))
    var result: DataFrame = bids.withColumn("rank", rank.over(price)).where("rank = 1").drop("rank")
    result

  }

}
