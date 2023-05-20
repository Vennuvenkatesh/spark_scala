package org.example

import org.apache.spark.sql.SparkSession

object CreateDataframe {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("CreateDataframe").getOrCreate()
    import spark.implicits._
    val data = Seq(("hadoop", "3000")).toDF("course", "fee")
    data.show(1000, 200, true)
    data.printSchema()
  }

}
