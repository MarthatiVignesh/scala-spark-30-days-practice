import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day13SparkSQLBasics {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 13 - Spark SQL Basics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("====================================================")
    println("        DAY 13 - SPARK SQL BASICS")
    println("====================================================")

    // --------------------------------------------------
    // 1. CREATE DATAFRAME FROM CSV
    // --------------------------------------------------

    println("\n1. CREATE DATAFRAME FROM CSV")
    println("----------------------------------------")

    val customersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    customersDF.show(false)

    // --------------------------------------------------
    // 2. INSPECT SCHEMA
    // --------------------------------------------------

    println("\n2. DATAFRAME SCHEMA")
    println("----------------------------------------")

    customersDF.printSchema()

    // --------------------------------------------------
    // 3. SELECT COLUMNS
    // --------------------------------------------------

    println("\n3. SELECT CUSTOMER COLUMNS")
    println("----------------------------------------")

    customersDF
      .select("customer_id", "name", "city", "purchase_amount")
      .show(false)

    // --------------------------------------------------
    // 4. FILTER ACTIVE CUSTOMERS
    // --------------------------------------------------

    println("\n4. ACTIVE CUSTOMERS")
    println("----------------------------------------")

    val activeCustomersDF = customersDF
      .filter(col("status") === "Active")

    activeCustomersDF
      .select("customer_id", "name", "city", "purchase_amount")
      .show(false)

    // --------------------------------------------------
    // 5. FILTER HIGH VALUE CUSTOMERS
    // --------------------------------------------------

    println("\n5. HIGH VALUE CUSTOMERS")
    println("----------------------------------------")

    customersDF
      .filter(col("purchase_amount") >= 60000)
      .select("customer_id", "name", "purchase_amount")
      .show(false)

    // --------------------------------------------------
    // 6. WITHCOLUMN AND EXPRESSIONS
    // --------------------------------------------------

    println("\n6. WITHCOLUMN - CUSTOMER CATEGORY")
    println("----------------------------------------")

    val customerCategoryDF = customersDF
      .withColumn(
        "customer_category",
        when(col("purchase_amount") >= 80000, "Premium")
          .when(col("purchase_amount") >= 50000, "Regular")
          .otherwise("Basic")
      )

    customerCategoryDF
      .select(
        "customer_id",
        "name",
        "purchase_amount",
        "customer_category"
      )
      .show(false)

    // --------------------------------------------------
    // 7. WITHCOLUMN - PURCHASE WITH TAX
    // --------------------------------------------------

    println("\n7. WITHCOLUMN - PURCHASE WITH 18% TAX")
    println("----------------------------------------")

    val taxDF = customersDF
      .withColumn(
        "purchase_with_tax",
        round(col("purchase_amount") * 1.18, 2)
      )

    taxDF
      .select(
        "customer_id",
        "name",
        "purchase_amount",
        "purchase_with_tax"
      )
      .show(false)

    // --------------------------------------------------
    // 8. TEMPORARY VIEW
    // --------------------------------------------------

    println("\n8. CREATE TEMPORARY VIEW")
    println("----------------------------------------")

    customersDF.createOrReplaceTempView("customers")

    println("Temporary view 'customers' created successfully.")

    // --------------------------------------------------
    // 9. SPARK SQL - ACTIVE CUSTOMERS
    // --------------------------------------------------

    println("\n9. SPARK SQL - ACTIVE CUSTOMERS")
    println("----------------------------------------")

    val activeSQL = spark.sql(
      """
        |SELECT customer_id, name, city, purchase_amount
        |FROM customers
        |WHERE status = 'Active'
        |ORDER BY purchase_amount DESC
        |""".stripMargin
    )

    activeSQL.show(false)

    // --------------------------------------------------
    // 10. SPARK SQL - CITY-WISE ANALYTICS
    // --------------------------------------------------

    println("\n10. CITY-WISE CUSTOMER ANALYTICS")
    println("----------------------------------------")

    val cityAnalytics = spark.sql(
      """
        |SELECT
        |  city,
        |  COUNT(*) AS customer_count,
        |  ROUND(AVG(purchase_amount), 2) AS average_purchase,
        |  SUM(purchase_amount) AS total_purchase
        |FROM customers
        |GROUP BY city
        |ORDER BY total_purchase DESC
        |""".stripMargin
    )

    cityAnalytics.show(false)

    // --------------------------------------------------
    // 11. SPARK SQL - TOP CUSTOMERS
    // --------------------------------------------------

    println("\n11. TOP 5 CUSTOMERS")
    println("----------------------------------------")

    val topCustomers = spark.sql(
      """
        |SELECT
        |  customer_id,
        |  name,
        |  city,
        |  purchase_amount
        |FROM customers
        |ORDER BY purchase_amount DESC
        |LIMIT 5
        |""".stripMargin
    )

    topCustomers.show(false)

    // --------------------------------------------------
    // 12. CUSTOMER ANALYTICS REPORT
    // --------------------------------------------------

    println("\n12. CUSTOMER ANALYTICS REPORT")
    println("========================================")

    val totalCustomers = customersDF.count()

    val activeCount = customersDF
      .filter(col("status") === "Active")
      .count()

    val inactiveCount = customersDF
      .filter(col("status") === "Inactive")
      .count()

    val totalPurchase = customersDF
      .agg(sum("purchase_amount"))
      .first()
      .get(0)

    val averagePurchase = customersDF
      .agg(round(avg("purchase_amount"), 2))
      .first()
      .get(0)

    println(s"Total Customers     : $totalCustomers")
    println(s"Active Customers    : $activeCount")
    println(s"Inactive Customers  : $inactiveCount")
    println(s"Total Purchase      : ₹$totalPurchase")
    println(s"Average Purchase    : ₹$averagePurchase")

    println("\nCustomer Analytics Report generated successfully.")

    println("\n====================================================")
    println("       DAY 13 COMPLETED SUCCESSFULLY")
    println("====================================================")

    spark.stop()
  }
}
