import org.apache.spark.sql.SparkSession

object Day04RDDCreation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 4 - RDD Creation")
      .master("local[4]")
      .getOrCreate()

    val sc = spark.sparkContext

    println("===================================")
    println("DAY 4 - RDD CREATION")
    println("===================================")

    println(s"Spark Version: ${spark.version}")
    println(s"Master: ${sc.master}")
    println(s"Default Parallelism: ${sc.defaultParallelism}")

    // ------------------------------------------------
    // 1. RDD FROM SCALA COLLECTION
    // ------------------------------------------------

    println("\n--- 1. RDD From Scala Collection ---")

    val numbers = Seq(10, 20, 30, 40, 50)

    val numberRDD = sc.parallelize(numbers, 4)

    println("Original Collection: " + numbers.mkString(", "))
    println("RDD Values: " + numberRDD.collect().mkString(", "))
    println("RDD Partitions: " + numberRDD.getNumPartitions)

    // ------------------------------------------------
    // 2. MAP OPERATION
    // ------------------------------------------------

    println("\n--- 2. Map Operation ---")

    val doubledRDD = numberRDD.map(x => x * 2)

    println("Doubled Values: " + doubledRDD.collect().mkString(", "))

    // ------------------------------------------------
    // 3. FILTER OPERATION
    // ------------------------------------------------

    println("\n--- 3. Filter Operation ---")

    val filteredRDD = numberRDD.filter(x => x > 25)

    println("Values Greater Than 25: " +
      filteredRDD.collect().mkString(", "))

    // ------------------------------------------------
    // 4. FLATMAP OPERATION
    // ------------------------------------------------

    println("\n--- 4. FlatMap Operation ---")

    val wordsRDD = sc.parallelize(
      Seq("Apache Spark", "Scala Spark", "RDD Practice"),
      2
    )

    val flatWordsRDD = wordsRDD.flatMap(_.split(" "))

    println("FlatMap Result: " +
      flatWordsRDD.collect().mkString(", "))

    // ------------------------------------------------
    // 5. RDD FROM TEXT FILE
    // ------------------------------------------------

    println("\n--- 5. RDD From Text File ---")

    val customerFile = "data/customers.txt"

    val customerRDD = sc.textFile(customerFile, 4)

    println("Customer Records: " + customerRDD.count())
    println("Customer RDD Partitions: " +
      customerRDD.getNumPartitions)

    println("\nCustomer Data:")

    customerRDD.collect().foreach(println)

    // ------------------------------------------------
    // 6. MAP + FILTER ON CUSTOMER RDD
    // ------------------------------------------------

    println("\n--- 6. Customer Map and Filter ---")

    val hyderabadCustomers = customerRDD
      .map(line => line.split(","))
      .filter(fields => fields(2) == "Hyderabad")
      .map(fields => fields(0) + " - " + fields(1))

    println("Hyderabad Customers:")

    hyderabadCustomers.collect().foreach(println)

    // ------------------------------------------------
    // 7. SALES RDD
    // ------------------------------------------------

    println("\n--- 7. Total Sales Using RDD ---")

    val salesData = Seq(
      ("Laptop", 2, 60000.0),
      ("Mobile", 3, 25000.0),
      ("Headphones", 5, 4000.0),
      ("Keyboard", 4, 1500.0)
    )

    val salesRDD = sc.parallelize(salesData, 4)

    val totalSales = salesRDD
      .map {
        case (_, quantity, price) => quantity * price
      }
      .reduce(_ + _)

    println("Total Sales: Rs." + totalSales)
    println("Sales RDD Partitions: " +
      salesRDD.getNumPartitions)

    // ------------------------------------------------
    // 8. LARGE CUSTOMER FILE PARTITIONS
    // ------------------------------------------------

    println("\n--- 8. Customer File Partitioning ---")

    val partitionedCustomerRDD =
      sc.textFile(customerFile, 4)

    println("Customer Records: " +
      partitionedCustomerRDD.count())

    println("Requested Partitions: 4")
    println("Actual Partitions: " +
      partitionedCustomerRDD.getNumPartitions)

    println("\nPartition Information:")

    partitionedCustomerRDD
      .mapPartitionsWithIndex {
        (partitionIndex, iterator) => {

          val records = iterator.toList

          Iterator(
            s"Partition $partitionIndex -> ${records.size} records"
          )
        }
      }
      .collect()
      .foreach(println)

    // ------------------------------------------------
    // 9. TRANSFORMATIONS AND ACTIONS
    // ------------------------------------------------

    println("\n--- 9. Transformations and Actions ---")

    println("Transformations used:")
    println("map")
    println("filter")
    println("flatMap")

    println("\nActions used:")
    println("count")
    println("collect")
    println("reduce")

    println("\nPartitioning:")
    println("Customer RDD uses multiple partitions.")
    println("Partitions allow Spark to process data in parallel.")

    println("\n===================================")
    println("DAY 4 APPLICATION COMPLETED")
    println("===================================")

    spark.stop()
  }
}
