
import org.apache.spark.sql.SparkSession

object Joins {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local[*]").appName("Emp").getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val Emp=Seq((1, "Smith", -1, "2018", "10", "M", 3000),
      (2, "Rose", 1, "2010", "20", "M", 4000),
      (3, "Williams", 1, "2010", "10", "M", 1000),
      (4, "Jones", 2, "2005", "10", "F", 2000),
      (5, "Brown", 2, "2010", "40", "", -1),
      (6, "Brown", 2, "2010", "50", "", -1))

    val empcolumn = Seq("emp_id","name","superior_emp_id","year_joined","emp_dept_id","gender","salary")
    val empDF = Emp.toDF(empcolumn:_*)
    empDF.show(false)

    val dept = Seq(("Finance",10),
      ("Marketing",20),
      ("Sales",30),
      ("IT",40))
    val deptcolumns=Seq("dept_name","dept_id")
    val deptDF= dept.toDF(deptcolumns:_*)
    deptDF.show(false)
empDF.join(deptDF,
  empDF("emp_dept_id") === deptDF("dept_id"),
  "inner").select(empDF("*"),deptDF("dept_id")).show()

    empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),
      "left").select(empDF("*"),deptDF("dept_id")).show(false)

    //empDF.join(deptDF,empDF("emp_dept_id") === deptDF("dept_id"),
      //"right").
    //  select(empDF("*"),deptDF("dept_id")).collect()


//println("sample_df")

// val sampleDF =  empDF.as("df1")
//      .join(empDF.as("df2"),
//        $"df1.emp_id" === $"df2.superior_emp_id").select($"df1.superior_emp_id",$"df1.name", $"df2.name").
//   println("self join")
////   val selfJoinDf = empDF.as("emp1").join(empDF.as("emp2"),emp1($"superior_emp_id") === emp2($"emp_id"), "inner")("select(col(emp1.emp_id),col(emp1.name), col(emp2.emp_id).as(superior_emp_id), col(emp2.name).as(superior_emp_name)")
//



  }

  }





