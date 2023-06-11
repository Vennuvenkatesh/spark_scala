//package org.example
//
//import org.apache.spark.sql.SparkSession
//
//object repartitionandcoalesce {
//  val venky = SparkSession.builder()
//    .master("local[*]")
//    .appName("repartition and coalesce")
//    .getOrCreate()
//  val sc = venky.sparkContext
//
//  val ravi = sc.parallelize(1 to 2000)
//
//  ravi.count()

//
//  def main(args: Array[String]): Unit = {
//
//    println(ravi.partitions.length)
//
//    ravi.count()
//
//    val repartitionedNumbers = ravi.repartition(3)
//    println(ravi.partitions.length)
//
//    val coalescedNumbers = ravi.coalesce(2)
//    println(ravi.partitions.length)
//
//    ravi.count()
//    scala.io.StdIn.readLine()
//
//  }
//
//}
