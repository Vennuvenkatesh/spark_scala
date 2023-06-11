package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark
//
object dataframejoins {
  def main(args: Array[String]): Unit = {
    val product = SparkSession.builder().master("local[*]").appName("product").getOrCreate()
    import product.implicits._
    product.sparkContext.setLogLevel("ERROR")

    val df1 = Seq((101, "chilli", 22000), (102, "cotton", 7000),
      (103, "paddy", 2000), (104, "conflor", 1900),
      (105,"wheat",3400),(106,"pearl millet",4500))

    val df1column = Seq("product_id", "product_name", "price")
    val productdf = df1.toDF(df1column: _*)
    productdf.show(false)

    val orders = Seq((10158, "2023-01-23",103), (10346, "2023-02-13",103),
      (10589, "2023-03-21",102), (10889, "2023-05-12",102),
      (10984,"2023-04-12",110),(11056,"2023-06-09",112))
    val orderscolumn = Seq("order_id", "order_date","product_id")
    val ordersdf = orders.toDF(orderscolumn: _*)
    ordersdf.show(false)

    // inner joindf
       println("inner")
    productdf.join(ordersdf,
      productdf("product_id") === ordersdf("product_id"),
      "inner").select(productdf("*"),ordersdf("order_id"),ordersdf("order_date"))
      .show()


    // left joindf
      println("left")
   productdf.join(ordersdf,
      productdf ("product_id") === ordersdf ("product_id"),"left")
      .select(productdf("*"),ordersdf("order_id"),ordersdf("order_date"))
        .show(false)

    //right joindf
      println("right")
    productdf.join(ordersdf,productdf("product_id") === ordersdf ("product_id"),"right")
      .select(productdf("*"),ordersdf("order_id"),ordersdf("order_date")).show(false)

    // full outerjoin df

     println("fullouter")
    productdf.join(ordersdf,productdf("product_id") === ordersdf ("product_id"),"fullouter")
      .select(productdf("*"),ordersdf("order_id"),ordersdf("order_date")).show(false)

////
    // left semi join
    println("leftsemi")
  productdf.join(ordersdf,
    productdf("product_id") === ordersdf("product_id"), "leftsemi")
    .show(false)

    // left antijoin
     println("leftanti")
    productdf.join(ordersdf,productdf("product_id")  ===  ordersdf("product_id"),"left_anti")
      .show(false)


//


  }

}
