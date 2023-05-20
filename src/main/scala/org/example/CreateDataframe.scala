package org.example

import org.apache.spark.sql.SparkSession

object CreateDataframe {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("CreateDataframe").getOrCreate()
    import spark.implicits._
    val df = Seq(("hadoop", "3000")).toDF("course", "fee")
    df.show(1000, 200, true)
    df.printSchema()
  }

}
