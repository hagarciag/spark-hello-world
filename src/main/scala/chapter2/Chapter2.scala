package chapter2

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * A batch application that takes a hard-coded list of strings and counts the words.
 */
object Chapter2 {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "MyBatchApp"

  def main(args: Array[String]): Unit = {
    val sentences = Seq(
      "Space.",
      "The final frontier.",
      "These are the voyages of the starship Enterprise.",
      "Its continuing mission:",
      "to explore strange new worlds,",
      "to seek out new life and new civilizations,",
      "to boldly go where no one has gone before!"
    )

    try {
      // create our spark session which will run locally
      val spark = SparkSession.builder().appName(jobName).master("local[*]").getOrCreate()
      // create an RDD using our test data
      val myRange = spark.range(100).toDF("number")

      val divisBy2 = myRange.where("number % 2 = 0")

      println(divisBy2.count())

      val flightData2015 = spark
        .read
        .option("inferSchema", "true")
        .option("header", "true")
        //git clone https://github.com/hagarciag/Spark-The-Definitive-Guide.git
        //C:\Users\herna\IdeaProjects\flight-data\data\flight-data\csv
        //.csv("/data/flight-data/csv/2015-summary.csv")
        .csv("../flight-data/data/flight-data/csv/2015-summary.csv")

      println(flightData2015.take(3))

      println(flightData2015.sort("count").explain())

      spark.conf.set("spark.sql.shuffle.partitions", "5")

      println(flightData2015.sort("count").take(2))

      flightData2015.createOrReplaceTempView("flight_data_2015")

      val sqlWay = spark.sql(
        """
          |SELECT DEST_COUNTRY_NAME, count(1)
          |FROM flight_data_2015
          |GROUP BY DEST_COUNTRY_NAME
          |""".stripMargin)

      //sqlWay.show(truncate = false)

      val dataFrameWay = flightData2015
        .groupBy("DEST_COUNTRY_NAME")
        .count()

      //dataFrameWay.show(truncate = false)

      sqlWay.explain()
      dataFrameWay.explain()

      import org.apache.spark.sql.functions.{max, desc}
      flightData2015.select(max("count")).take(1)

      val maxSql = spark.sql("""
        SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
        FROM flight_data_2015
        GROUP BY DEST_COUNTRY_NAME
        ORDER BY sum(count) DESC
        LIMIT 5
        """)
      maxSql.show()

      flightData2015
        .groupBy("DEST_COUNTRY_NAME")
        .sum("count")
        .withColumnRenamed("sum(count)", "destination_total")
        .sort(desc("destination_total"))
        .limit(5)
        .explain()

    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

  def splitSentenceIntoWords(sentence: String): Array[String] = {
    sentence.split(" ").map(word => word.toLowerCase.replaceAll("[^a-z]", ""))
  }

}
