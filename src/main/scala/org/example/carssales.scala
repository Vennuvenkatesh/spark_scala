package org.example

import org.apache.spark.sql.SparkSession

//object carssales {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().master("local[*]").appName("cars").getOrCreate()
//
//    import spark.implicits._
//    spark.sparkContext.setLogLevel("ERROR")
//   val df = spark.read.format("csv").option("header",true).load("C:\\Users\\91984\\Downloads\\Customer_Data.csv")
//    df.show(1000,false)
//
//    val df1 =spark.read.format("csv").option("header",true).load("C:\\Users\\91984\\Downloads\\Orders_Data.csv")
//
//    df1.show(1000,false)
//
//    //joins
//
//    //inner join
//
//   df.join(df1,df("customer_id") === )










   // val partitions_Count = df.rdd.getNumPartitions
     // println(df.rdd.getNumPartitions)
   // println(partitions_Count)

    // repartition creations

    //val repatition = df.repartition(10)

    //println(repatition.rdd.getNumPartitions)


    //scala.io.StdIn.readLine()

// }
//
//}
