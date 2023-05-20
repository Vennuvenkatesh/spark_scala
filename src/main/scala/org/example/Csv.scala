import org.apache.spark.sql.SparkSession

object Csv {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("csv").getOrCreate()
    val data = Seq(("hadoop",4500),("spark",5000))
    import spark.implicits._
    val df = data.toDF("course","price")
    df.show()
    //df.printSchema()
  }

}
