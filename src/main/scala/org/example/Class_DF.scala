import org.apache.spark.sql.SparkSession

object Class_DF {
  def main(args: Array[String]): Unit = {
     val spark = SparkSession.builder().master("local[*]").appName("emp").getOrCreate()
   import spark.implicits._

    val empdf = Seq((1,"ravi","sales",10000),

      (2,"gopi","product",20000),

      (3,"ashok","chilly",23000),

      (4,"venky","cotton",23400))
      .toDF("id","Name","dept","amount")

    empdf.show()

    val filterDf = empdf.where($"id" === "1")

    filterDf.show()


  }


}
