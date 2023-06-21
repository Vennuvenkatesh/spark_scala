package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.expressions.Window


object Healthcare_Analytics {
  def main(args: Array[String]): Unit = {
    val patients = SparkSession.builder().master("local[*]").appName("patients").getOrCreate()
    import patients.implicits._
    val df = Seq((1, "john Smith", 45, "Male", "Seattle"),
      (2, "Jane Doe", 32, "Female", "Miami"),
      (3, "Mike Johnson", 50, "Male", "Seattle"),
      (4, "Lisa Johnson", 28, "Female", "Miami"),
      (5, "David Kim", 60, "Male", "Chicago"))
    val patients_columns = Seq("patient_id", "patient_name", "age", "gender", "city")
    val patient_df = df.toDF(patients_columns: _*)
    patient_df.show(false)
    patients.sparkContext.setLogLevel("ERROR")

    val symptoms = Seq((1, "Fever"), (2, "Cough"), (3, "Breathing"),
      (4, "Fatigue"), (5, "Headache"))
    val symptoms_column = Seq("symptoms_id", "symptoms_name")
    val symptoms_df = symptoms.toDF(symptoms_column: _*)
    symptoms_df.show(false)


    val Diagnoses = Seq((1, "Common Cold"), (2, "Influenza"), (3, "Pneumonia"), (4, "Bronchitis"), (5, "COVID-19"))
    val Diagnoses_column = Seq("Diagnoses_id", "Diagnoses_name")
    val Diagnoses_df = Diagnoses.toDF(Diagnoses_column: _*)
    Diagnoses_df.show(false)

    val visit = Seq((1, 1, 1, 2, "2022-01-01"), (2, 2, 2, 1, "2022-01-02"), (3, 3, 3, 3, "2022-01-02"),
      (4, 4, 1, 4, "2022-01-03"), (5, 5, 2, 5, "2022-01-03"), (6, 1, 4, 1, "2022-05-13"),
      (7, 3, 4, 1, "2022-05-20"), (8, 3, 2, 1, "2022-05-20"), (9, 2, 1, 4, "2022-08-19"))
    val visits_column = Seq("visit_id", "patient_id", "symptoms_id", "Diagnoses_id", "visit_date")
    val visits_df = visit.toDF(visits_column: _*)
    visits_df.show(false)

    //1.write a sql query to retrieve all patients who have been diagnosed with covid-19
    import org.apache.spark.sql.functions
    println("covid_patients")
    val covid_patients = patient_df.join(visits_df, patient_df("patient_id") === visits_df("patient_id"), "inner")
      .join(Diagnoses_df, visits_df("Diagnoses_id") === Diagnoses_df("Diagnoses_id"), "inner")
      .join(symptoms_df, visits_df("symptoms_id") === symptoms_df("symptoms_id"), "inner")
      .select(patient_df("patient_id"), patient_df("patient_name"), symptoms_df("symptoms_name"), Diagnoses_df("Diagnoses_name"))
    val covid_with_patients = covid_patients.filter($"Diagnoses_name" === "COVID-19")

    covid_with_patients.show()

    //2.write a sql query to retrieve the no of visits made by each patient ,order by no of visits in desc order?

    val orders_visits = patient_df.join(visits_df, patient_df("patient_id") === visits_df("patient_id"), "inner")
    val group_by_patients = orders_visits.groupBy(patient_df("patient_name")).count().alias("no_of_visits")
    val orders_df = group_by_patients.orderBy($"count".desc).select(patient_df("patient_name"), $"count".alias("no_of_visits"))
    orders_df.show(false)

    // 3.write a sql query to calculate the average age of patients who have been diagnosed with pneumonia?

    println("avg_agedf")
   val patients_fd = visits_df.join(patient_df, visits_df("patient_id") === patient_df("patient_id"), "inner")
  .join(Diagnoses_df, visits_df("Diagnoses_id") === Diagnoses_df("Diagnoses_id"), "inner")
    val avg_agedf = patients_fd
      .filter($"Diagnoses_name" === "Pneumonia")
      .select(avg(patient_df("age")).alias("avg_age"))
    avg_agedf.show()


//4.write a sql query to retrieve the top 3 most common symptoms among all visits.
    val topThreeSymptoms = visits_df
      .join(symptoms_df, visits_df("symptoms_id") === symptoms_df("symptoms_id"), "inner")
      .groupBy(symptoms_df("symptoms_name"))
      .count()
      .orderBy(desc("count"))
      .limit(3)
    topThreeSymptoms.show()


     //5.write a sql query to retrieve the patient who has the highest no of patients different symptoms reported.

     val Heighest_Symptoms = patient_df.join(visits_df,patient_df("patient_id") === visits_df("patient_id"),"inner")
     .join(symptoms_df,visits_df("symptoms_id") === symptoms_df("symptoms_id"),"inner")
       .groupBy(patient_df("patient_name"))
       .count()
       .orderBy(desc("count"),countDistinct("patient_name")).limit(1)
    Heighest_Symptoms.show()

    //6.write a sql query to calculate the percentage of patients who have been diagnosed covid-19 out of the total no of patients.



    val totalPatients = patient_df.count()
    val covidPatients = patient_df
      .join(visits_df, patient_df("patient_id") === visits_df("patient_id"))
      .join(Diagnoses_df, visits_df("Diagnoses_id") === Diagnoses_df("Diagnoses_id"))
      .filter($"Diagnoses_name" === "COVID-19")
      .distinct()
      .count()
    val percentage = (covidPatients.toDouble / totalPatients.toDouble) * 100

    println(s"The percentage of patients diagnosed with COVID-19: $percentage%")

    //7.write a sql query to retrieve the top 5 cities with the heighest number of visits,along with the count of visits in each city.

    println("top_five_cities")
    val top_five_cities  =  patient_df.join(visits_df,patient_df("patient_id") === visits_df("patient_id"),"inner")
      .groupBy("city")
      .count().orderBy(desc("count"))
      .limit(5)
      .select(col("city").as("city"),col("count").as("visits_per_city"))
      .show()

  //8.write a sql query to find the patient who has the heighest number of visits in single day,along with corresponding visit date.

    val Heighest_number_of_visits_in_single_day = patient_df.join(visits_df,patient_df("patient_id") === visits_df("patient_id"))
      .groupBy("patient_name","visit_date")
      .count()
      .orderBy(desc("count"))
      .select(col("patient_name").as("patient_name"),
        col("count").as("visit_count"),col("visit_date").as("visit_per_day"))
    .limit(1).show()

    //9.write a sql query to retrieve the average age of patients for each diagnosis,ordered by the average age in desc order?


    val average_age_of_patients = patient_df.join(visits_df,patient_df("patient_id") === visits_df("patient_id"),"inner")
      .join(Diagnoses_df,visits_df("Diagnoses_id") === Diagnoses_df("Diagnoses_id"),"inner")
    .groupBy(Diagnoses_df("Diagnoses_name"))
      .agg(avg("age").alias("avg_age"))
       .orderBy(desc("avg_age"))
      .show()
     //10.write a sql query to calculate the cumulative count visits over time,ordered by the visit date.


    val windowSpec = Window.orderBy("visit_date").rowsBetween(Window.unboundedPreceding, 0)

    val cumulative_count_visits = visits_df
      .groupBy("visit_date")
      .count()
      .orderBy(desc("visit_date"))
      .withColumn("cumulative_count", sum("count").over(windowSpec))
      .select(col("visit_date"), col("cumulative_count"))

    cumulative_count_visits.show()
     scala.io.StdIn.readLine()
  }

}
