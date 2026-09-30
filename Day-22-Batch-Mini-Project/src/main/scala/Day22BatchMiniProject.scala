import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Day22BatchMiniProject {

  def main(args: Array[String]): Unit = {

    // ------------------------------------------------------------
    // 1. CREATE SPARK SESSION
    // ------------------------------------------------------------

    val spark = SparkSession.builder()
      .appName("Day 22 - E-Commerce Batch Mini Project")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("\n================================================")
    println("       DAY 22 - BATCH MINI PROJECT")
    println("       E-COMMERCE DAILY SALES PIPELINE")
    println("================================================")


    // ------------------------------------------------------------
    // 2. INPUT PATHS
    // ------------------------------------------------------------

    val transactionsPath = "data/transactions.csv"
    val customersPath = "data/customers.csv"
    val productsPath = "data/products.csv"


    // ------------------------------------------------------------
    // 3. READ RAW TRANSACTIONS
    // ------------------------------------------------------------

    println("\n1. READING RAW TRANSACTIONS")
    println("-----------------------------------------------")

    val transactions = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("dateFormat", "yyyy-MM-dd")
      .csv(transactionsPath)

    println(s"Raw transaction count: ${transactions.count()}")

    transactions.show(false)
    transactions.printSchema()


    // ------------------------------------------------------------
    // 4. READ CUSTOMER DATA
    // ------------------------------------------------------------

    println("\n2. READING CUSTOMER DATA")
    println("-----------------------------------------------")

    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(customersPath)

    println(s"Customer count: ${customers.count()}")

    customers.show(false)


    // ------------------------------------------------------------
    // 5. READ PRODUCT DATA
    // ------------------------------------------------------------

    println("\n3. READING PRODUCT DATA")
    println("-----------------------------------------------")

    val products = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(productsPath)

    println(s"Product count: ${products.count()}")

    products.show(false)


    // ------------------------------------------------------------
    // 6. CLEAN INVALID TRANSACTIONS
    // ------------------------------------------------------------

    println("\n4. CLEANING INVALID TRANSACTIONS")
    println("-----------------------------------------------")

    val validTransactions = transactions
      .filter(col("status") === "Completed")
      .filter(col("amount") > 0)
      .filter(col("quantity") > 0)

    println(
      s"Valid transactions after basic cleaning: ${validTransactions.count()}"
    )

    println("\nRemoved during basic validation:")

    transactions
      .filter(
        (col("status") =!= "Completed") ||
        (col("amount") <= 0) ||
        (col("quantity") <= 0)
      )
      .show(false)


    // ------------------------------------------------------------
    // 7. JOIN CUSTOMER DATA
    // ------------------------------------------------------------

    println("\n5. JOINING CUSTOMER DATA")
    println("-----------------------------------------------")

    val customerJoined = validTransactions
      .join(
        customers,
        validTransactions("customer_id") === customers("customer_id"),
        "inner"
      )
      .select(
        validTransactions("transaction_id"),
        validTransactions("customer_id"),
        customers("customer_name"),
        customers("city"),
        customers("segment"),
        validTransactions("product_id"),
        validTransactions("transaction_date"),
        validTransactions("quantity"),
        validTransactions("amount")
      )

    println(s"Records after customer join: ${customerJoined.count()}")

    customerJoined.show(false)


    // ------------------------------------------------------------
    // 8. JOIN PRODUCT DATA
    // ------------------------------------------------------------

    println("\n6. JOINING PRODUCT DATA")
    println("-----------------------------------------------")

    val enrichedTransactions = customerJoined
      .join(
        products,
        customerJoined("product_id") === products("product_id"),
        "inner"
      )
      .select(
        customerJoined("transaction_id"),
        customerJoined("customer_id"),
        customerJoined("customer_name"),
        customerJoined("city"),
        customerJoined("segment"),
        customerJoined("product_id"),
        products("product_name"),
        products("category"),
        customerJoined("transaction_date"),
        customerJoined("quantity"),
        customerJoined("amount")
      )

    println(
      s"Records after customer + product joins: ${enrichedTransactions.count()}"
    )

    enrichedTransactions.show(false)


    // ------------------------------------------------------------
    // 9. DAILY REVENUE AGGREGATION
    // ------------------------------------------------------------

    println("\n7. DAILY REVENUE AGGREGATION")
    println("-----------------------------------------------")

    val dailyRevenue = enrichedTransactions
      .groupBy("transaction_date")
      .agg(
        count("transaction_id").alias("transaction_count"),
        sum("quantity").alias("total_quantity"),
        sum("amount").alias("total_revenue")
      )
      .orderBy("transaction_date")

    dailyRevenue.show(false)


    // ------------------------------------------------------------
    // 10. PRODUCT REVENUE SUMMARY
    // ------------------------------------------------------------

    println("\n8. PRODUCT REVENUE SUMMARY")
    println("-----------------------------------------------")

    val productRevenue = enrichedTransactions
      .groupBy("product_id", "product_name", "category")
      .agg(
        count("transaction_id").alias("transaction_count"),
        sum("quantity").alias("total_quantity"),
        sum("amount").alias("total_revenue")
      )
      .orderBy(desc("total_revenue"))

    productRevenue.show(false)


    // ------------------------------------------------------------
    // 11. CITY REVENUE SUMMARY
    // ------------------------------------------------------------

    println("\n9. CITY REVENUE SUMMARY")
    println("-----------------------------------------------")

    val cityRevenue = enrichedTransactions
      .groupBy("city")
      .agg(
        count("transaction_id").alias("transaction_count"),
        sum("amount").alias("total_revenue")
      )
      .orderBy(desc("total_revenue"))

    cityRevenue.show(false)


    // ------------------------------------------------------------
    // 12. ADD PARTITION COLUMNS
    // ------------------------------------------------------------

    println("\n10. PREPARING PARTITIONED OUTPUT")
    println("-----------------------------------------------")

    val partitionedRevenue = dailyRevenue
      .withColumn("year", year(col("transaction_date")))
      .withColumn("month", month(col("transaction_date")))
      .withColumn("day", dayofmonth(col("transaction_date")))

    partitionedRevenue.show(false)


    // ------------------------------------------------------------
    // 13. WRITE PARTITIONED PARQUET
    // ------------------------------------------------------------

    println("\n11. WRITING PARTITIONED PARQUET")
    println("-----------------------------------------------")

    val outputPath = "output/daily-revenue"

    partitionedRevenue
      .repartition(col("year"), col("month"), col("day"))
      .write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet(outputPath)

    println(s"Partitioned Parquet written to: $outputPath")


    // ------------------------------------------------------------
    // 14. READ OUTPUT BACK
    // ------------------------------------------------------------

    println("\n12. READING PARTITIONED PARQUET")
    println("-----------------------------------------------")

    val finalOutput = spark.read
      .parquet(outputPath)

    println(s"Final output records: ${finalOutput.count()}")

    finalOutput
      .orderBy("transaction_date")
      .show(false)


    // ------------------------------------------------------------
    // 15. EXPLAIN EXECUTION PLAN
    // ------------------------------------------------------------

    println("\n13. EXECUTION PLAN")
    println("-----------------------------------------------")

    dailyRevenue.explain("formatted")


    // ------------------------------------------------------------
    // 16. FINAL PROJECT EVIDENCE
    // ------------------------------------------------------------

    println("\n\n================================================")
    println("             FINAL PROJECT EVIDENCE")
    println("================================================")

    println(
      s"Raw Transactions          : ${transactions.count()}"
    )

    println(
      s"Valid Transactions        : ${validTransactions.count()}"
    )

    println(
      s"After Customer Join       : ${customerJoined.count()}"
    )

    println(
      s"After Product Join        : ${enrichedTransactions.count()}"
    )

    println(
      s"Daily Revenue Records     : ${dailyRevenue.count()}"
    )

    println(
      s"Final Parquet Records     : ${finalOutput.count()}"
    )

    println("\nPipeline Completed:")
    println("1. Read raw transactions")
    println("2. Cleaned invalid records")
    println("3. Joined customer data")
    println("4. Joined product data")
    println("5. Aggregated revenue")
    println("6. Created partition columns")
    println("7. Wrote partitioned Parquet")
    println("8. Read output back")
    println("9. Verified execution plan")

    println("\nDAY 22 COMPLETED SUCCESSFULLY")


    // ------------------------------------------------------------
    // 17. STOP SPARK
    // ------------------------------------------------------------

    spark.stop()
  }
}
