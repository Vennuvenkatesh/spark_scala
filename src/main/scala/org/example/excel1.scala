import org.apache.spark.sql.SparkSession

object excel1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("excel1").getOrCreate()

     spark.sparkContext.setLogLevel("ERROR")

    val excel_Df = spark.read.format("Csv")

      .load("C:\\Users\\91984\\Downloads\\MOCKDATA.xlsx")


    excel_Df.show(1000, false)

//    scala.io.StdIn.readLine()
  }

}
