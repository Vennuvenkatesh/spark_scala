package org.example
//
//import org.apache.spark.sql.{SaveMode, SparkSession}
//
//object sparkdataframe {
//
//  val spark = SparkSession.builder()
//    .master("local[*]")
//    .appName("repartition and coalesce")
//    .getOrCreate()
//  val sc = spark.sparkContext
//  val numbers = sc.parallelize (1 to 1000)
//  def main(args: Array[String]): Unit = {
//
//    println(numbers.partitions.length)
//
//    val repartitionNumbers = numbers.repartition(2)
//    repartitionNumbers.count()
//
//    val coalescedNumbers = numbers.coalesce(2)
//
//    coalescedNumbers.count()
//
//
//    scala.io.StdIn.readLine()
//
//
//
//  }
//
//}
