import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day16Aggregations {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 16 - Aggregations")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("\n===== DAY 16 - AGGREGATIONS =====")

    // --------------------------------------------------
    // 1. Read hospital revenue data
    // --------------------------------------------------
    val hospitalDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/hospital_revenue.csv")

    println("\n--- Hospital Revenue Data ---")
    hospitalDF.show(false)

    println("\n--- Schema ---")
    hospitalDF.printSchema()

    // --------------------------------------------------
    // 2. Basic Aggregations
    // --------------------------------------------------
    println("\n--- Basic Aggregations ---")

    val basicStats = hospitalDF.agg(
      count("*").alias("patient_count"),
      sum("revenue").alias("total_revenue"),
      avg("revenue").alias("average_revenue"),
      min("revenue").alias("minimum_revenue"),
      max("revenue").alias("maximum_revenue")
    )

    basicStats.show(false)

    // --------------------------------------------------
    // 3. Department-wise aggregations
    // --------------------------------------------------
    println("\n--- Department-Wise Revenue Statistics ---")

    val departmentStats = hospitalDF
      .groupBy("department")
      .agg(
        count("*").alias("patient_count"),
        sum("revenue").alias("total_revenue"),
        avg("revenue").alias("average_revenue"),
        min("revenue").alias("minimum_revenue"),
        max("revenue").alias("maximum_revenue")
      )
      .orderBy("department")

    departmentStats.show(false)

    // --------------------------------------------------
    // 4. GroupBy with multiple columns
    // --------------------------------------------------
    println("\n--- Department and Visit Type Statistics ---")

    val departmentVisitStats = hospitalDF
      .groupBy("department", "visit_type")
      .agg(
        count("*").alias("visit_count"),
        sum("revenue").alias("total_revenue"),
        avg("revenue").alias("average_revenue")
      )
      .orderBy("department", "visit_type")

    departmentVisitStats.show(false)

    // --------------------------------------------------
    // 5. HAVING-like filtering
    // --------------------------------------------------
    println("\n--- Departments With Total Revenue > 50000 ---")

    val highRevenueDepartments = departmentStats
      .filter(col("total_revenue") > 50000)
      .orderBy(desc("total_revenue"))

    highRevenueDepartments.show(false)

    // --------------------------------------------------
    // 6. Doctor-wise revenue
    // --------------------------------------------------
    println("\n--- Doctor-Wise Revenue Statistics ---")

    val doctorStats = hospitalDF
      .groupBy("doctor")
      .agg(
        count("*").alias("patient_count"),
        sum("revenue").alias("total_revenue"),
        avg("revenue").alias("average_revenue")
      )
      .orderBy(desc("total_revenue"))

    doctorStats.show(false)

    // --------------------------------------------------
    // 7. Overall revenue statistics
    // --------------------------------------------------
    val totalPatients = hospitalDF.count()

    val totalRevenue = hospitalDF
      .agg(sum("revenue"))
      .first()
      .getLong(0)

    val averageRevenue = hospitalDF
      .agg(avg("revenue"))
      .first()
      .getDouble(0)

    val minimumRevenue = hospitalDF
      .agg(min("revenue"))
      .first()
      .getInt(0)

    val maximumRevenue = hospitalDF
      .agg(max("revenue"))
      .first()
      .getInt(0)

    println("\n--- Final Statistics ---")
    println(s"Total Patients : $totalPatients")
    println(s"Total Revenue : $totalRevenue")
    println(s"Average Revenue : $averageRevenue")
    println(s"Minimum Revenue : $minimumRevenue")
    println(s"Maximum Revenue : $maximumRevenue")

    println("\nDAY 16 COMPLETED SUCCESSFULLY")

    spark.stop()
  }
}
