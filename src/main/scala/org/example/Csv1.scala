import org.apache.spark.sql.SparkSession

object Csv1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Csv read").getOrCreate()
    val csvdf = spark.read.option("header",true).option("inferschema",true).csv("C:\\Users\\91984\\Downloads\\MOCK_DATA.csv")

//    csvdf.printSchema()
//    csvdf.show(1000,false)
//    scala.io.StdIn.readLine()
  }



}
