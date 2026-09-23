import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class Employee(
  employee_id: String,
  name: String,
  department: String,
  age: Int,
  salary: Double
)

case class Payroll(
  employee_id: String,
  name: String,
  department: String,
  salary: Double,
  annual_salary: Double,
  bonus: Double,
  total_pay: Double
)

object Day14DataFrameDataset {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 14 - DataFrame and Dataset")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    println("====================================================")
    println("DAY 14 - DATAFRAME AND DATASET")
    println("====================================================")

    // ------------------------------------------------
    // 1. Read CSV as DataFrame
    // ------------------------------------------------
    val employeeDF: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    println("\n--- 1. EMPLOYEE DATAFRAME ---")
    employeeDF.show(false)

    println("--- DATAFRAME SCHEMA ---")
    employeeDF.printSchema()

    // ------------------------------------------------
    // 2. Convert DataFrame to Dataset
    // ------------------------------------------------
    val employeeDS = employeeDF.as[Employee]

    println("\n--- 2. EMPLOYEE DATASET ---")
    employeeDS.show(false)

    // ------------------------------------------------
    // 3. Typed Dataset operation
    // ------------------------------------------------
    println("\n--- 3. TYPED DATASET: SALARY >= 65000 ---")

    val highSalaryEmployees = employeeDS
      .filter(employee => employee.salary >= 65000)

    highSalaryEmployees.show(false)

    // ------------------------------------------------
    // 4. Convert Dataset back to DataFrame
    // ------------------------------------------------
    val employeeDFAgain = employeeDS.toDF()

    println("\n--- 4. DATASET BACK TO DATAFRAME ---")
    employeeDFAgain.show(false)

    // ------------------------------------------------
    // 5. Typed Payroll Pipeline
    // ------------------------------------------------
    println("\n--- 5. TYPED EMPLOYEE PAYROLL PIPELINE ---")

    val payrollDS = employeeDS.map { employee =>

      val annualSalary = employee.salary * 12
      val bonus =
        if (employee.salary >= 80000) employee.salary * 0.10
        else employee.salary * 0.05

      val totalPay = annualSalary + bonus

      Payroll(
        employee.employee_id,
        employee.name,
        employee.department,
        employee.salary,
        annualSalary,
        bonus,
        totalPay
      )
    }

    payrollDS.show(false)

    // ------------------------------------------------
    // 6. Payroll DataFrame
    // ------------------------------------------------
    println("\n--- 6. PAYROLL DATAFRAME ---")

    val payrollDF = payrollDS.toDF()

    payrollDF
      .select(
        "employee_id",
        "name",
        "department",
        "annual_salary",
        "bonus",
        "total_pay"
      )
      .show(false)

    // ------------------------------------------------
    // 7. Department-wise payroll
    // ------------------------------------------------
    println("\n--- 7. DEPARTMENT-WISE PAYROLL ---")

    payrollDF
      .groupBy("department")
      .agg(
        count("*").alias("employee_count"),
        round(sum("annual_salary"), 2).alias("total_annual_salary"),
        round(sum("bonus"), 2).alias("total_bonus"),
        round(sum("total_pay"), 2).alias("total_payroll")
      )
      .orderBy(desc("total_payroll"))
      .show(false)

    // ------------------------------------------------
    // 8. RDD, DataFrame and Dataset comparison
    // ------------------------------------------------
    println("\n--- 8. RDD vs DATAFRAME vs DATASET ---")

    println("RDD       : Low-level distributed collection, flexible and object-oriented.")
    println("DataFrame : Distributed table of Rows with schema and SQL/DataFrame APIs.")
    println("Dataset   : Typed distributed collection with compile-time type safety.")
    println("Catalyst  : Spark SQL optimizer used for DataFrame/Dataset query plans.")
    println("Type Safety: Dataset provides compile-time checking for typed operations.")

    // ------------------------------------------------
    // 9. Final statistics
    // ------------------------------------------------
    println("\n--- 9. FINAL PAYROLL STATISTICS ---")

    val employeeCount = employeeDS.count()
    val totalSalary = employeeDS
      .agg(round(sum("salary"), 2).alias("total_salary"))
      .first()
      .getLong(0)

    val averageSalary = employeeDS
      .agg(round(avg("salary"), 2).alias("average_salary"))
      .first()
      .getDouble(0)

    println(s"Total Employees : $employeeCount")
    println(s"Total Monthly Salary : $totalSalary")
    println(s"Average Monthly Salary : $averageSalary")

    println("\n====================================================")
    println("DAY 14 COMPLETED SUCCESSFULLY")
    println("====================================================")

    spark.stop()
  }
}
