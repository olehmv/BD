package module4.homework1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MotelsHomeRecommendation {

  import org.apache.spark.rdd.RDD

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation"))

    processData(sc, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sc: SparkContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Hint: Use the BideError case class
      */
    val erroneousRecords: RDD[String] = getErroneousRecords(rawBids)
    erroneousRecords.saveAsTextFile(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: Map[String, Double] = getExchangeRates(sc, exchangeRatesPath)

    /**
      * Task 3:
      * Transform the rawBids and use the BidItem case class.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: RDD[BidItem] = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: RDD[(String, String)] = getMotels(sc, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price.
      */
    val enriched: RDD[EnrichedItem] = getEnriched(bids, motels)
    enriched.saveAsTextFile(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = sc.textFile(bidsPath).map(s => s.split(",").map(s => s.trim).toList).cache()

  def getErroneousRecords(rawBids: RDD[List[String]]): RDD[String] = {
    var errors: RDD[BidError] = rawBids.filter(s => s.toList.toString().contains("ERROR_")).map(s => BidError(s(1), s(2)))
    var countErrors: RDD[(BidError, Int)] = errors.groupBy(b => b).mapValues(_.size)
    var result: RDD[String] = countErrors.map { case (error, count) => error.toString + "," + count }
    result
  }

  def getExchangeRates(sc: SparkContext, exchangeRatesPath: String): Map[String, Double] =
    sc.textFile(exchangeRatesPath).map(s => s.split(",").map(s => s.trim())).map(s => (s(0), s(3).toDouble)).collect().toMap


  def getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double]): RDD[BidItem] = {
    import java.io.{BufferedWriter, FileWriter}
    var value: RDD[List[String]] = rawBids.filter(list => !list(2).startsWith("ERROR_"))
    convertToBidItems(value, exchangeRates)
  }


  def convertToBidItems(bidsData: RDD[List[String]], exchangeRates: Map[String, Double]): RDD[BidItem] = {
    def convert(line: List[String]): List[BidItem] = {
      var list: List[BidItem] = List()
      var bids: List[String] = line.drop(2)
      val motelIdAndDate: List[String] = line.take(2)
      var indexesOfTargetLosas: Seq[Int] = Constants.TARGET_LOSAS.map(item => Constants.BIDS_HEADER.indexOf(item) - 2)
      var count = 0;
      for (bid <- bids) {
        var indexOfBid = count
        var priceInUS: Option[Double] = toDouble(bid)
        var priceFormatIsGood: Boolean = !priceInUS.equals(None)
        if (indexesOfTargetLosas.contains(indexOfBid) && priceFormatIsGood) {
          var originalDate: String = motelIdAndDate(1).trim
          var convertedDate: String = convertDateFormat(originalDate)
          var motelId: String = motelIdAndDate(0)
          var loSa: String = Constants.BIDS_HEADER(indexOfBid + 2)
          var priceInEUR: Double = convertCurrencyApplyRate(exchangeRates, originalDate, priceInUS.get)
          var item: BidItem = BidItem(motelId, convertedDate, loSa, priceInEUR)
          list = item :: list
        }
        count = count + 1
      }
      var tuples0: List[(String, BidItem)] = List()
      list.map {
        item =>
          if (item.loSa.equals("US")) {
            tuples0 = ("A", item) :: tuples0
          }
          if (item.loSa == "CA") {
            tuples0 = ("B", item) :: tuples0
          }
          if (item.loSa == "MX") {
            tuples0 = ("C", item) :: tuples0
          }
      }
      var tuples01: List[(String, BidItem)] = tuples0.sortBy(_._1)
      var result: List[BidItem] = tuples01.map {
        case (a, b) => b
      }.toList
      result
    }

    bidsData.map(line => convert(line)).flatMap(item => item)
  }

  def toDouble(s: String): Option[Double] = {
    try {
      Some(s.toDouble)
    } catch {
      case e: NumberFormatException => None
    }
  }

  def convertDateFormat(str: String): String = {
    val date = Constants.INPUT_DATE_FORMAT.parseDateTime(str)
    date.toString(Constants.OUTPUT_DATE_FORMAT)
  }

  def convertCurrencyApplyRate(exchangeRates: Map[String, Double], date: String, price: Double): Double = {
    var rate: Option[Double] = exchangeRates.get(date)
    var result: Double = BigDecimal(price * rate.get).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
    result
  }

  def getEnriched(bids: RDD[BidItem], motels: RDD[(String, String)]): RDD[EnrichedItem] = {
    import scala.reflect.ClassTag
    var bidItems: RDD[(String, BidItem)] = bids.map(item => (item.motelId, item))
    var joinItems: RDD[(String, (BidItem, String))] = bidItems.join(motels)
    var enrichItems: RDD[EnrichedItem] = joinItems.map {
      case (_, (bidItem, motelName)) =>
        EnrichedItem(bidItem.motelId, motelName, bidItem.bidDate, bidItem.loSa, bidItem.price)
    }
    var result: RDD[(String, Iterable[EnrichedItem])] = enrichItems.groupBy(item => item.motelId + item.bidDate)
    result.map {
      case (_, bids) => bids.reduceLeft((item1, item2) => if (item1.price >= item2.price) item1 else item2)
    }
  }

  def getMotels(sc: SparkContext, motelsPath: String): RDD[(String, String)] = {
    sc.textFile(motelsPath).map {
      s =>
        var arr: Array[String] = s.split(",")
        var motelId: String = arr(0)
        var motelName: String = arr(1)
        (motelId, motelName)
    }
  }
}
