import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Day17WindowFunctions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day-17-Window-Functions")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ============================================================
    // PART 1: STUDENT DATA
    // ============================================================

    println("\n========== STUDENT DATA ==========")

    val students = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/students.csv")

    students.show(false)

    // Window partitioned by course
    val courseWindow =
      Window.partitionBy("course")
        .orderBy(col("marks").desc)

    // row_number
    val studentsWithRowNumber = students.withColumn(
      "row_number",
      row_number().over(courseWindow)
    )

    println("\n========== ROW_NUMBER ==========")
    studentsWithRowNumber.show(false)

    // rank
    val studentsWithRank = students.withColumn(
      "rank",
      rank().over(courseWindow)
    )

    println("\n========== RANK ==========")
    studentsWithRank.show(false)

    // dense_rank
    val studentsWithDenseRank = students.withColumn(
      "dense_rank",
      dense_rank().over(courseWindow)
    )

    println("\n========== DENSE_RANK ==========")
    studentsWithDenseRank.show(false)

    // Top 3 students per course
    val top3Students = studentsWithDenseRank
      .filter(col("dense_rank") <= 3)

    println("\n========== TOP 3 STUDENTS PER COURSE ==========")
    top3Students
      .select(
        "student_id",
        "student_name",
        "course",
        "marks",
        "dense_rank"
      )
      .orderBy(col("course"), col("dense_rank"), col("marks").desc)
      .show(false)

    // ============================================================
    // PART 2: CUSTOMER POLICY DATA
    // ============================================================

    println("\n========== CUSTOMER POLICY DATA ==========")

    val policies = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customer_policies.csv")

    policies.show(false)

    // Window partitioned by customer and ordered by policy date
    val customerWindow =
      Window.partitionBy("customer_id")
        .orderBy(col("policy_date"))

    // Latest policy using row_number
    val latestPolicyWindow =
      Window.partitionBy("customer_id")
        .orderBy(col("policy_date").desc)

    val policiesWithLatestRank = policies.withColumn(
      "latest_rank",
      row_number().over(latestPolicyWindow)
    )

    println("\n========== LATEST POLICY PER CUSTOMER ==========")

    policiesWithLatestRank
      .filter(col("latest_rank") === 1)
      .select(
        "customer_id",
        "customer_name",
        "policy_id",
        "policy_type",
        "policy_status",
        "policy_amount",
        "policy_date"
      )
      .orderBy("customer_id")
      .show(false)

    // ============================================================
    // PART 3: LAG AND LEAD
    // ============================================================

    val policiesWithLagLead = policies
      .withColumn(
        "previous_policy_amount",
        lag("policy_amount", 1).over(customerWindow)
      )
      .withColumn(
        "next_policy_amount",
        lead("policy_amount", 1).over(customerWindow)
      )

    println("\n========== LAG AND LEAD ==========")

    policiesWithLagLead
      .select(
        "customer_id",
        "customer_name",
        "policy_id",
        "policy_amount",
        "policy_date",
        "previous_policy_amount",
        "next_policy_amount"
      )
      .orderBy("customer_id", "policy_date")
      .show(false)

    // ============================================================
    // PART 4: FINAL STATISTICS
    // ============================================================

    println("\n========== FINAL STATISTICS ==========")

    val totalStudents = students.count()
    val totalPolicies = policies.count()
    val totalCourses = students.select("course").distinct().count()
    val totalCustomers = policies.select("customer_id").distinct().count()

    println(s"Total Students  : $totalStudents")
    println(s"Total Policies  : $totalPolicies")
    println(s"Total Courses   : $totalCourses")
    println(s"Total Customers : $totalCustomers")

    println("\nDAY 17 COMPLETED SUCCESSFULLY")

    spark.stop()
  }
}
