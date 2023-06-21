package org.example

import org.apache.spark.sql.SparkSession

object chilli {
  def main(args: Array[String]): Unit = {
    val spark= SparkSession.builder().master("local[*]").appName("chilli").getOrCreate()
    import spark.implicits._ feature7/venky
    val df = Seq((1,"Teja",25000),(2,"Komali",26000),(3,"Bullet",22000),(4,"Bangaram",21000),(5,"Byadagi",32000),(6,"yello",32000))
    val df = Seq((1,"Teja",25000),(2,"Komali",26000),(3,"Bullet",22000),(4,"Bangaram",21000),(5,"Byadagi",32000),(6,"yellow",32000)) main

        .toDF("id","Type","Cost")
        df.show(false)
  }

}
//git commi -m "commiting chilli.scala" src/main/scala/org/example/chilli.scalaZZ