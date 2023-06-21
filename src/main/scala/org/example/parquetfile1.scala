//package org.example
//
//import org.apache.spark.sql.{SaveMode, SparkSession}
//
//object parquetfile1 {
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder().master("local[*]").appName("emp").getOrCreate
//
//    import spark.implicits._
//
//    val venky = Seq((1,"vsr",25000),(2,"vpr",12300),(3,"vvr",45221),(4,"vmr",43210),(5,"siva",54321),(6,"dvr",54324))
//
//    val df =venky.toDF("id","name","salary")
//
//    df.write
//      .parquet("example.parquet")
//
//  }
//
//}
