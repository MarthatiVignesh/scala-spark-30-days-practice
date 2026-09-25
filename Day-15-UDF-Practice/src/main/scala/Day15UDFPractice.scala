import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day15UDFPractice {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 15 - UDF Practice")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("\n===== DAY 15 - UDF PRACTICE =====")

    // --------------------------------------------------
    // 1. Read transaction data
    // --------------------------------------------------
    val transactionsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/transactions.csv")

    println("\n--- Input Transactions ---")
    transactionsDF.show(false)

    println("\n--- Input Schema ---")
    transactionsDF.printSchema()

    // --------------------------------------------------
    // 2. Create Scala UDF for risk classification
    // --------------------------------------------------
    val classifyRisk = udf((amount: Double) => {
      if (amount < 5000)
        "LOW"
      else if (amount < 20000)
        "MEDIUM"
      else if (amount < 80000)
        "HIGH"
      else
        "CRITICAL"
    })

    // --------------------------------------------------
    // 3. Use UDF with withColumn
    // --------------------------------------------------
    val riskDF = transactionsDF.withColumn(
      "risk_category",
      classifyRisk(col("transaction_amount"))
    )

    println("\n--- UDF: Customer Risk Classification ---")
    riskDF.show(false)

    // --------------------------------------------------
    // 4. Built-in Spark function
    // --------------------------------------------------
    val amountWithTaxDF = transactionsDF.withColumn(
      "transaction_with_tax",
      round(col("transaction_amount") * 1.18, 2)
    )

    println("\n--- Built-in Spark Function: Transaction + 18% Tax ---")
    amountWithTaxDF.show(false)

    // --------------------------------------------------
    // 5. Compare UDF with built-in function
    // --------------------------------------------------
    println("\n--- UDF vs Built-in Function ---")

    println("UDF:")
    println("  Used for custom business logic such as risk classification.")

    println("Built-in Function:")
    println("  Used for standard operations such as arithmetic and rounding.")

    println("Performance:")
    println("  Built-in Spark functions are generally preferred because")
    println("  Spark can optimize them more effectively.")

    // --------------------------------------------------
    // 6. Register UDF with Spark SQL Catalog
    // --------------------------------------------------
    spark.udf.register(
      "classify_risk",
      (amount: Double) => {
        if (amount < 5000)
          "LOW"
        else if (amount < 20000)
          "MEDIUM"
        else if (amount < 80000)
          "HIGH"
        else
          "CRITICAL"
      }
    )

    // --------------------------------------------------
    // 7. Create temporary view
    // --------------------------------------------------
    transactionsDF.createOrReplaceTempView("transactions")

    // --------------------------------------------------
    // 8. Use registered UDF through Spark SQL
    // --------------------------------------------------
    val sqlRiskDF = spark.sql("""
      SELECT
        customer_id,
        customer_name,
        transaction_amount,
        classify_risk(transaction_amount) AS risk_category
      FROM transactions
    """)

    println("\n--- Registered UDF using Spark SQL ---")
    sqlRiskDF.show(false)

    // --------------------------------------------------
    // 9. Risk category summary
    // --------------------------------------------------
    val riskSummaryDF = riskDF
      .groupBy("risk_category")
      .count()
      .orderBy("risk_category")

    println("\n--- Risk Category Summary ---")
    riskSummaryDF.show(false)

    // --------------------------------------------------
    // 10. Final statistics
    // --------------------------------------------------
    val totalCustomers = transactionsDF.count()

    val totalAmount = transactionsDF
      .agg(sum("transaction_amount"))
      .first()
      .getLong(0)

    println("\n--- Final Statistics ---")
    println(s"Total Customers : $totalCustomers")
    println(s"Total Transaction Amount : $totalAmount")

    println("\nDAY 15 COMPLETED SUCCESSFULLY")

    spark.stop()
  }
}
