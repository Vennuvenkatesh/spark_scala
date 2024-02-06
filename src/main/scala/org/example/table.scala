//package org.example
//
//import org.apache.spark.sql.SparkSession
//
//object table {
//  def main(args: Array[String]): Unit = {
//    val Spark = SparkSession.builder().master("local[*]").appName("ravi").getOrCreate()
//    import Spark.implicits._
//    val sampleDF = Seq(
//      (1, "Tiger"),
//      (2, "Lion"),
//      (3, "Monkey")
//    ).toDF("id", "animal").show()
//  }
//
//}
object CurryingExample {
  def add(x: Int)(y: Int): Int = x + y

  def main(args: Array[String]): Unit = {
    // Partially applying the 'add' function
    val addTwo = add(2)_

    // Using the partially applied function
    val result1 = addTwo(3) // result1 is 5
    val result2 = addTwo(10) // result2 is 12

    println(s"Result 1: $result1")
    println(s"Result 2: $result2")
  }
}
