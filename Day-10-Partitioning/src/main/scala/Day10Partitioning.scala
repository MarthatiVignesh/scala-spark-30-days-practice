import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner

object Day10Partitioning {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 10 - Partitioning")
      .master("local[4]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    println("==================================================")
    println("DAY 10 - SPARK PARTITIONING")
    println("==================================================")

    // --------------------------------------------------
    // 1. Read the sales dataset
    // --------------------------------------------------

    val sales = sc.textFile("data/sales.txt")

    println("\n1. ORIGINAL DATASET")
    println("-------------------")
    println(s"Total records: ${sales.count()}")
    println(s"Original partitions: ${sales.getNumPartitions}")

    // --------------------------------------------------
    // 2. Inspect records in each partition
    // --------------------------------------------------

    println("\n2. RECORDS PER ORIGINAL PARTITION")
    println("---------------------------------")

    val originalPartitionInfo = sales.mapPartitionsWithIndex {
      (partitionId, records) =>
        Iterator(s"Partition $partitionId -> ${records.size} records")
    }.collect()

    originalPartitionInfo.foreach(println)

    // --------------------------------------------------
    // 3. Repartition
    // --------------------------------------------------

    val repartitionedSales = sales.repartition(4)

    println("\n3. REPARTITION")
    println("--------------")
    println(s"Partitions after repartition(4): ${repartitionedSales.getNumPartitions}")

    val repartitionInfo = repartitionedSales.mapPartitionsWithIndex {
      (partitionId, records) =>
        Iterator(s"Partition $partitionId -> ${records.size} records")
    }.collect()

    repartitionInfo.foreach(println)

    // --------------------------------------------------
    // 4. Coalesce
    // --------------------------------------------------

    val coalescedSales = repartitionedSales.coalesce(2)

    println("\n4. COALESCE")
    println("-----------")
    println(s"Partitions after coalesce(2): ${coalescedSales.getNumPartitions}")

    val coalesceInfo = coalescedSales.mapPartitionsWithIndex {
      (partitionId, records) =>
        Iterator(s"Partition $partitionId -> ${records.size} records")
    }.collect()

    coalesceInfo.foreach(println)

    // --------------------------------------------------
    // 5. Too Few Partitions Scenario
    // --------------------------------------------------

    val tooFewPartitions = sc.parallelize(sales.collect(), 1)

    println("\n5. TOO FEW PARTITIONS SCENARIO")
    println("------------------------------")
    println(s"Records: ${tooFewPartitions.count()}")
    println(s"Partitions: ${tooFewPartitions.getNumPartitions}")
    println("Problem: One partition may become a bottleneck.")
    println("Solution: Increase partitions using repartition().")

    val optimizedSales = tooFewPartitions.repartition(4)

    println(s"Optimized partitions: ${optimizedSales.getNumPartitions}")

    // --------------------------------------------------
    // 6. Pair RDD + partitionBy
    // --------------------------------------------------

    val productSales = sales.map { line =>
      val parts = line.split(",")
      val product = parts(0)
      val amount = parts(2).toDouble
      (product, amount)
    }

    println("\n6. PAIR RDD")
    println("-----------")
    println(s"Pair RDD partitions before partitionBy: ${productSales.getNumPartitions}")

    val partitionedProductSales =
      productSales.partitionBy(new HashPartitioner(4))

    println(s"Pair RDD partitions after partitionBy(4): ${partitionedProductSales.getNumPartitions}")

    val partitionedInfo = partitionedProductSales.mapPartitionsWithIndex {
      (partitionId, records) =>
        Iterator(s"Partition $partitionId -> ${records.size} records")
    }.collect()

    partitionedInfo.foreach(println)

    // --------------------------------------------------
    // 7. Revenue by Product
    // --------------------------------------------------

    val revenueByProduct = partitionedProductSales
      .reduceByKey(_ + _)

    println("\n7. REVENUE BY PRODUCT")
    println("--------------------")

    revenueByProduct
      .collect()
      .sortBy(-_._2)
      .foreach {
        case (product, revenue) =>
          println(f"$product%-10s -> ₹$revenue%.0f")
      }

    // --------------------------------------------------
    // 8. When to increase/decrease partitions
    // --------------------------------------------------

    println("\n8. WHEN TO CHANGE PARTITIONS")
    println("----------------------------")
    println("Increase partitions when:")
    println("- Dataset is large")
    println("- Partitions are too few")
    println("- Some tasks are processing too much data")
    println("- More parallelism is needed")

    println("\nDecrease partitions when:")
    println("- Dataset becomes smaller")
    println("- Too many small partitions exist")
    println("- Reducing task scheduling overhead is useful")

    // --------------------------------------------------
    // 9. Repartition vs Coalesce
    // --------------------------------------------------

    println("\n9. REPARTITION VS COALESCE")
    println("--------------------------")
    println("repartition(n): Can increase or decrease partitions.")
    println("                 Uses a shuffle.")
    println("coalesce(n):    Mainly decreases partitions.")
    println("                 Usually avoids a full shuffle.")

    // --------------------------------------------------
    // 10. Transformations and Actions
    // --------------------------------------------------

    println("\n10. TRANSFORMATIONS AND ACTIONS")
    println("-------------------------------")
    println("Transformations:")
    println("- repartition")
    println("- coalesce")
    println("- map")
    println("- mapPartitionsWithIndex")
    println("- partitionBy")
    println("- reduceByKey")

    println("\nActions:")
    println("- count")
    println("- collect")

    println("\nLazy operations are transformations.")
    println("Spark builds the execution plan and runs it when an action is called.")

    println("\n==================================================")
    println("DAY 10 COMPLETED SUCCESSFULLY")
    println("==================================================")

    spark.stop()
  }
}
