import org.apache.spark.sql.SparkSession

object Day09PairRDD {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day-09-Pair-RDD")
      .master("local[4]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    println("============================================================")
    println("              DAY 09 - PAIR RDD PRACTICE")
    println("============================================================")

    // ----------------------------------------------------------
    // 1. CREATE KEY-VALUE RDD
    // ----------------------------------------------------------

    println("\n1. KEY-VALUE RDD")

    val numbers = sc.parallelize(
      Seq(
        ("A", 10),
        ("B", 20),
        ("A", 30),
        ("B", 40),
        ("C", 50)
      ),
      4
    )

    println("Original Pair RDD:")
    numbers.collect().foreach(println)


    // ----------------------------------------------------------
    // 2. reduceByKey()
    // ----------------------------------------------------------

    println("\n2. reduceByKey()")

    val reduced = numbers.reduceByKey(_ + _)

    println("Sum by key:")
    reduced.collect().sortBy(_._1).foreach(println)


    // ----------------------------------------------------------
    // 3. groupByKey()
    // ----------------------------------------------------------

    println("\n3. groupByKey()")

    val grouped = numbers.groupByKey()

    println("Values grouped by key:")
    grouped.collect().sortBy(_._1).foreach {
      case (key, values) =>
        println(s"$key -> ${values.toList.sorted.mkString(", ")}")
    }


    // ----------------------------------------------------------
    // 4. mapValues()
    // ----------------------------------------------------------

    println("\n4. mapValues()")

    val doubled = numbers.mapValues(value => value * 2)

    println("Values multiplied by 2:")
    doubled.collect().sortBy(_._1).foreach(println)


    // ----------------------------------------------------------
    // 5. PRODUCT REVENUE
    // ----------------------------------------------------------

    println("\n5. REVENUE BY PRODUCT")

    val sales = sc.textFile("data/sales.txt")

    val productRevenue = sales
      .map { line =>
        val parts = line.split(",")
        val product = parts(0)
        val amount = parts(2).toDouble
        (product, amount)
      }

    val revenueByProduct = productRevenue
      .reduceByKey(_ + _)

    println("Revenue by product:")

    revenueByProduct
      .collect()
      .sortBy(-_._2)
      .foreach {
        case (product, revenue) =>
          println(f"$product%-10s -> ₹$revenue%.2f")
      }


    // ----------------------------------------------------------
    // 6. REVENUE BY DEPARTMENT
    // ----------------------------------------------------------

    println("\n6. REVENUE BY DEPARTMENT")

    val departmentRevenue = sales
      .map { line =>
        val parts = line.split(",")
        val department = parts(1)
        val amount = parts(2).toDouble
        (department, amount)
      }
      .reduceByKey(_ + _)

    println("Revenue by department:")

    departmentRevenue
      .collect()
      .sortBy(-_._2)
      .foreach {
        case (department, revenue) =>
          println(f"$department%-12s -> ₹$revenue%.2f")
      }


    // ----------------------------------------------------------
    // 7. reduceByKey() vs groupByKey()
    // ----------------------------------------------------------

    println("\n7. reduceByKey() vs groupByKey()")

    println("reduceByKey():")
    println("- Performs local aggregation before shuffle.")
    println("- Transfers less data across the network.")
    println("- Usually more efficient for aggregation.")

    println("\ngroupByKey():")
    println("- Groups all values belonging to each key.")
    println("- More data may be transferred during shuffle.")
    println("- Can use more memory for large datasets.")

    println("\nPerformance comparison:")
    println("reduceByKey() -> preferred for sum/count/aggregation.")
    println("groupByKey()  -> useful when all individual values are required.")


    // ----------------------------------------------------------
    // 8. BANK TRANSACTION AGGREGATION
    // ----------------------------------------------------------

    println("\n8. BANK TRANSACTIONS BY ACCOUNT ID")

    val transactions = sc.textFile("data/bank_transactions.txt")

    val accountTransactions = transactions.map { line =>
      val parts = line.split(",")

      val accountId = parts(0)
      val transactionType = parts(1)
      val amount = parts(2).toDouble

      (accountId, (transactionType, amount))
    }

    println("Transaction records:")
    accountTransactions.collect()
      .sortBy(_._1)
      .foreach(println)


    // ----------------------------------------------------------
    // 9. CALCULATE NET BALANCE BY ACCOUNT
    // ----------------------------------------------------------

    println("\n9. NET BALANCE BY ACCOUNT")

    val netBalance = transactions
      .map { line =>
        val parts = line.split(",")

        val accountId = parts(0)
        val transactionType = parts(1)
        val amount = parts(2).toDouble

        val signedAmount =
          if (transactionType == "Deposit")
            amount
          else
            -amount

        (accountId, signedAmount)
      }
      .reduceByKey(_ + _)

    println("Net balance:")
    netBalance
      .collect()
      .sortBy(_._1)
      .foreach {
        case (account, balance) =>
          println(f"$account -> ₹$balance%.2f")
      }


    // ----------------------------------------------------------
    // 10. PARTITION INFORMATION
    // ----------------------------------------------------------

    println("\n10. PARTITION INFORMATION")

    println(s"Sales RDD partitions: ${sales.getNumPartitions}")
    println(
      s"Product revenue RDD partitions: ${revenueByProduct.getNumPartitions}"
    )
    println(
      s"Department revenue RDD partitions: ${departmentRevenue.getNumPartitions}"
    )
    println(
      s"Bank transaction RDD partitions: ${transactions.getNumPartitions}"
    )


    // ----------------------------------------------------------
    // 11. TRANSFORMATIONS AND ACTIONS
    // ----------------------------------------------------------

    println("\n11. TRANSFORMATIONS AND ACTIONS")

    println("Transformations:")
    println("- map")
    println("- mapValues")
    println("- reduceByKey")
    println("- groupByKey")

    println("\nActions:")
    println("- collect")
    println("- foreach")

    println("\nLazy Operations:")
    println("- map")
    println("- mapValues")
    println("- reduceByKey")
    println("- groupByKey")

    println("\n============================================================")
    println("              DAY 09 COMPLETED SUCCESSFULLY")
    println("============================================================")

    spark.stop()
  }
}
