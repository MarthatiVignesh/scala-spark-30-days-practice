import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day20FileFormatsOutput {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day20FileFormatsOutput")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ------------------------------------------------------------
    // 1. READ CSV
    // ------------------------------------------------------------

    println("\n================ READ CSV ================")

    val sales = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/daily_sales.csv")

    sales.show(false)

    println(s"CSV Records: ${sales.count()}")

    // ------------------------------------------------------------
    // 2. WRITE CSV
    // ------------------------------------------------------------

    println("\n================ WRITE CSV ================")

    sales.write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/csv")

    println("CSV output written to: output/csv")

    // ------------------------------------------------------------
    // 3. WRITE JSON
    // ------------------------------------------------------------

    println("\n================ WRITE JSON ================")

    sales.write
      .mode("overwrite")
      .json("output/json")

    println("JSON output written to: output/json")

    // ------------------------------------------------------------
    // 4. WRITE PARQUET
    // ------------------------------------------------------------

    println("\n================ WRITE PARQUET ================")

    sales.write
      .mode("overwrite")
      .parquet("output/parquet")

    println("Parquet output written to: output/parquet")

    // ------------------------------------------------------------
    // 5. READ BACK PARQUET
    // ------------------------------------------------------------

    println("\n================ READ PARQUET ================")

    val parquetSales = spark.read
      .parquet("output/parquet")

    parquetSales.show(false)

    println(s"Parquet Records: ${parquetSales.count()}")

    // ------------------------------------------------------------
    // 6. ADD YEAR / MONTH / DAY COLUMNS
    // ------------------------------------------------------------

    println("\n================ ADD PARTITION COLUMNS ================")

    val partitionedSales = sales
      .withColumn("sale_date", to_date(col("sale_date")))
      .withColumn("year", year(col("sale_date")))
      .withColumn("month", month(col("sale_date")))
      .withColumn("day", dayofmonth(col("sale_date")))

    partitionedSales.show(false)

    // ------------------------------------------------------------
    // 7. REPARTITION BEFORE WRITING
    // ------------------------------------------------------------

    println("\n================ REPARTITION ================")

    val repartitionedSales = partitionedSales
      .repartition(col("year"), col("month"), col("day"))

    println(
      s"Partitions after repartition: ${repartitionedSales.rdd.getNumPartitions}"
    )

    // ------------------------------------------------------------
    // 8. WRITE PARTITIONED PARQUET
    // ------------------------------------------------------------

    println("\n================ WRITE PARTITIONED PARQUET ================")

    repartitionedSales.write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet("output/partitioned-sales")

    println("Partitioned Parquet output written to:")
    println("output/partitioned-sales")

    // ------------------------------------------------------------
    // 9. FILE LAYOUT EXPLANATION
    // ------------------------------------------------------------

    println("\n================ FILE LAYOUT ================")

    println("Partitioned output follows the structure:")
    println("year=<value>/month=<value>/day=<value>/")
    println("Each Spark partition can produce an output part file.")
    println("The number of output files depends on the number of Spark partitions.")
    println("Repartition controls the distribution of records before writing.")

    // ------------------------------------------------------------
    // 10. READ PARTITIONED PARQUET
    // ------------------------------------------------------------

    println("\n================ READ PARTITIONED PARQUET ================")

    val partitionedRead = spark.read
      .parquet("output/partitioned-sales")

    partitionedRead.show(false)

    println(s"Partitioned Parquet Records: ${partitionedRead.count()}")

    // ------------------------------------------------------------
    // 11. PARTITION SUMMARY
    // ------------------------------------------------------------

    println("\n================ PARTITION SUMMARY ================")

    partitionedRead
      .groupBy("year", "month", "day")
      .agg(
        count("*").alias("record_count"),
        sum("amount").alias("total_amount")
      )
      .orderBy("year", "month", "day")
      .show(false)

    // ------------------------------------------------------------
    // 12. FINAL PROJECT EVIDENCE
    // ------------------------------------------------------------

    println("\n================ FINAL PROJECT EVIDENCE ================")

    println(s"Input CSV Records           : ${sales.count()}")
    println(s"Parquet Records             : ${parquetSales.count()}")
    println(s"Partitioned Parquet Records : ${partitionedRead.count()}")
    println(
      s"Partitions after repartition: ${repartitionedSales.rdd.getNumPartitions}"
    )

    println("\nFormats written successfully:")
    println("1. CSV")
    println("2. JSON")
    println("3. Parquet")
    println("4. Partitioned Parquet")

    println("\nDAY 20 COMPLETED SUCCESSFULLY")

    spark.stop()
  }
}
