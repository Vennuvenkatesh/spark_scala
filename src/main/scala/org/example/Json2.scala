import org.apache.spark.sql.SparkSession

object Json2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()

    val jsondf = spark.read.option("multiline",true).json("C:\\Users\\91984\\Downloads\\MOCK_DATA.xlsx")

//    jsondf.printSchema()
//
//    jsondf.show(1000,false)
//
//    scala.io.StdIn.readLine()
//

  }
}
