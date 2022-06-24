package chapter3

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * A batch application that takes a hard-coded list of strings and counts the words.
 */
object Chapter3 {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "MyBatchApp"
  case class Flight(DEST_COUNTRY_NAME: String,
                    ORIGIN_COUNTRY_NAME: String,
                    count: BigInt)


  def main(args: Array[String]): Unit = {

    try {
      // create our spark session which will run locally
      val spark = SparkSession.builder().appName(jobName).master("local[*]").getOrCreate()
      /*
      val flightsDF = spark.read
        .parquet("../flight-data/data/flight-data/parquet/2010-summary.parquet")
      */
      val flightsDF = spark
        .read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv("../flight-data/data/flight-data/csv/2010-summary.csv")


      import spark.implicits._

      val flights: Dataset[Flight] = flightsDF.as[Flight]

      val out = flights
        .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
        .map(fr => Flight(fr.ORIGIN_COUNTRY_NAME, fr.DEST_COUNTRY_NAME, fr.count + 5))
        .take(5)

      println(out)

      //Structured Streaming
      val staticDataFrame = spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("../flight-data/data/retail-data/by-day/*.csv")

      staticDataFrame.createOrReplaceTempView("retail_data")
      val staticSchema = staticDataFrame.schema

      import org.apache.spark.sql.functions.{col, window}
      staticDataFrame
        .selectExpr(
          "CustomerId",
          "(UnitPrice * Quantity) as total_cost",
          "InvoiceDate"
        )
        .groupBy(
          col("CustomerId"), window(col("InvoiceDate"), "1 day")
        )
        .sum("total_cost")
        .show(5)

      spark.conf.set("spark.sql.shuffle.partitions", "5")

      val streamingDataFrame = spark.readStream
        .schema(staticSchema)
        .option("maxFilesPerTrigger", 1)
        .option("format", "csv")
        .option("header", "true")
        .load("../flight-data/data/retail-data/by-day/*.csv")

      println(streamingDataFrame.isStreaming)


      val purchaseByCustomerPerHour = streamingDataFrame
        .selectExpr(
          "CustomerId",
          "(UnitPrice * Quantity) as total_cost",
          "InvoiceDate"
        )
        .groupBy(
          $"CustomerId", window(col("InvoiceDate"), "1 day")
        )
        .sum("total_cost")
/*
      purchaseByCustomerPerHour.writeStream
        .format("memory") // memory = store in-memory table
        .queryName("customer_purchases") // the name of the in-memory table
        .outputMode("complete") // complete = all the counts should be in the table
        .start()
      */
/*
      spark.sql("""
        SELECT *
        FROM customer_purchases
        ORDER BY `sum(total_cost)` DESC
        """)
        .show(5)*/
/*
      purchaseByCustomerPerHour.writeStream
        .format("console")
        .queryName("customer_purchases_2")
        .outputMode("complete")
        .start()
*/
      //Machine Learning and Advanced Analytics

    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

  def splitSentenceIntoWords(sentence: String): Array[String] = {
    sentence.split(" ").map(word => word.toLowerCase.replaceAll("[^a-z]", ""))
  }

}
