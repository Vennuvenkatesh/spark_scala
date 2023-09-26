
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object chilli {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("chilli")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(
      (1, "Teja", 25000),
      (2, "Komali", 26000),
      (3, "Bullet", 22000),
      (4, "Bangaram", 21000),
      (5, "Byadagi", 32000),
      (6, "yellow", 32000)
    ).toDF("id", "Type", "Cost")

    val floatValues = Array(23.5f, 39.1f, 34.7f, 29.0f, 26.8f, 35.9f)

    val updatedDF = df.withColumn("quantity", lit(floatValues(0)))
      .withColumn("quantity", when($"id" === 2, lit(floatValues(1))).otherwise($"quantity"))
      .withColumn("quantity", when($"id" === 3, lit(floatValues(2))).otherwise($"quantity"))
      .withColumn("quantity", when($"id" === 4, lit(floatValues(3))).otherwise($"quantity"))
      .withColumn("quantity", when($"id" === 5, lit(floatValues(4))).otherwise($"quantity"))
      .withColumn("quantity", when($"id" === 6, lit(floatValues(5))).otherwise($"quantity"))

    updatedDF.show()
//
    val dates = Seq(
      "2023-07-01",
      "2023-07-02",
      "2023-07-03",
      "2023-07-04",
      "2023-07-05",
      "2023-07-06"
    )

    val dfWithOrderDate = updatedDF
      .withColumn("Order_Date",
       when($"id" === 1, lit("2023-07-01"))
      .when($"id" === 2, lit("2023-07-02"))
      .when($"id" === 3, lit("2023-07-03"))
      .when($"id" === 4, lit("2023-07-04"))
      .when($"id" === 5, lit("2023-07-05"))
      .when($"id" === 6, lit("2023-07-06")))

    dfWithOrderDate.show()


    import org.apache.spark.sql.functions.{when, lit}

    val Dates = Seq(
      "2023-07-01",
      "2023-07-02",
      "2023-07-03",
      "2023-07-04",
      "2023-07-05",
      "2023-07-06")

    val invoice_Date = dfWithOrderDate
      .withColumn("invoice_Date"
        ,when($"id" === 1, lit("2023-07-01"))
          .when($"id" === 2, lit("2023-07-02"))
          .when($"id" === 3, lit("2023-07-03"))
          .when($"id" === 4, lit("2023-07-04"))
          .when($"id" === 5, lit("2023-07-05"))
          .when($"id" === 6, lit("2023-07-06")))

         invoice_Date.show(false)

  scala.io.StdIn.readLine()



  }
}