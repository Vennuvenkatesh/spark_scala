package org.example

import org.apache.spark.sql.SparkSession

object chilli {
  def main(args: Array[String]): Unit = {
    val spark= SparkSession.builder().master("local[*]").appName("chilli").getOrCreate()
    import spark.implicits._
    val df = Seq((1,"Teja",25000),(2,"Komali",26000),(3,"Bullet",22000),(4,"Bangaram",21000),(5,"Byadagi",32000),(6,"yello",32000))

        .toDF("id","Type","Cost")
        //df.show(false)
  }

}
