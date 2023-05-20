import org.apache.spark.sql.SparkSession

object Excelread {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("excelfileread").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val excelread = spark.read
      .format("com.crealytics.spark.excel")

      .option("header", "true")

      .option("inferSchema", "true")

      .load("C:\\Users\\91984\\Downloads\\MOCKDATA.xlsx")

//    excelread.show()
  }
}
