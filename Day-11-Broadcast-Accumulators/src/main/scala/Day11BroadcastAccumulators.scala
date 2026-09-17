import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object Day11BroadcastAccumulators {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 11 - Broadcast and Accumulators")
      .master("local[4]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    println("============================================================")
    println("DAY 11 - BROADCAST AND ACCUMULATORS")
    println("============================================================")

    // ------------------------------------------------------------
    // 1. Read small product master table
    // ------------------------------------------------------------

    val productMaster = sc.textFile("data/product_master.txt")
      .map { line =>
        val parts = line.split(",")
        (parts(0), (parts(1), parts(2)))
      }
      .collect()
      .toMap

    println("\n1. PRODUCT MASTER DATA")

    productMaster.toSeq.sortBy(_._1).foreach {
      case (id, (name, department)) =>
        println(s"$id -> $name -> $department")
    }

    // ------------------------------------------------------------
    // 2. Broadcast the small reference map
    // ------------------------------------------------------------

    val broadcastProductMaster = sc.broadcast(productMaster)

    println("\n2. BROADCAST VARIABLE")
    println("Product master map has been broadcast to executors.")
    println(s"Broadcast records: ${broadcastProductMaster.value.size}")

    // ------------------------------------------------------------
    // 3. Create accumulator for bad records
    // ------------------------------------------------------------

    val badRecords: LongAccumulator =
      sc.longAccumulator("Bad Records")

    // ------------------------------------------------------------
    // 4. Read transactions
    // ------------------------------------------------------------

    val transactions =
      sc.textFile("data/transactions.txt")

    // ------------------------------------------------------------
    // 5. Validate transactions using broadcast data
    // ------------------------------------------------------------

    val validatedTransactions = transactions.map { line =>

      val parts = line.split(",")

      val transactionId = parts(0)
      val productId = parts(1)
      val amount = parts(2).toDouble

      broadcastProductMaster.value.get(productId) match {

        case Some((productName, department)) =>

          s"VALID,$transactionId,$productId,$productName,$department,₹$amount"

        case None =>

          badRecords.add(1)

          s"INVALID,$transactionId,$productId,Unknown Product,Unknown,₹$amount"
      }
    }

    // Cache the RDD so the accumulator is not updated again
    // when the RDD is used by multiple actions.
    validatedTransactions.cache()

    // ------------------------------------------------------------
    // 6. Action triggers execution
    // ------------------------------------------------------------

    val results = validatedTransactions.collect()

    println("\n3. TRANSACTION VALIDATION")

    results.foreach(println)

    // ------------------------------------------------------------
    // 7. Display accumulator result
    // ------------------------------------------------------------

    println("\n4. ACCUMULATOR RESULT")
    println(s"Bad records found: ${badRecords.value}")

    // ------------------------------------------------------------
    // 8. Filter valid transactions
    // ------------------------------------------------------------

    val validTransactions =
      results.filter(_.startsWith("VALID"))

    println("\n5. VALID TRANSACTIONS")

    validTransactions.foreach(println)

    // ------------------------------------------------------------
    // 9. Filter invalid transactions
    // ------------------------------------------------------------

    val invalidTransactions =
      results.filter(_.startsWith("INVALID"))

    println("\n6. INVALID TRANSACTIONS")

    invalidTransactions.foreach(println)

    // ------------------------------------------------------------
    // 10. Department revenue using valid transactions
    // ------------------------------------------------------------

    val departmentRevenue = validatedTransactions
      .filter(_.startsWith("VALID"))
      .map { line =>

        val parts = line.split(",")

        val department = parts(4)
        val amount = parts(5).replace("₹", "").toDouble

        (department, amount)
      }
      .reduceByKey(_ + _)
      .collect()
      .sortBy(_._1)

    println("\n7. VALID REVENUE BY DEPARTMENT")

    departmentRevenue.foreach {
      case (department, revenue) =>
        println(f"$department%-15s ₹$revenue%.2f")
    }

    // ------------------------------------------------------------
    // 11. Explain normal driver variable
    // ------------------------------------------------------------

    println("\n8. WHY NOT USE A NORMAL DRIVER VARIABLE?")

    println("A normal driver variable is local to the driver.")
    println("Executors process data independently on distributed tasks.")
    println("Updates made by executors to a normal driver variable")
    println("are not reliably sent back to the driver.")
    println("Spark accumulators are designed for distributed counters.")

    // ------------------------------------------------------------
    // 12. Summary
    // ------------------------------------------------------------

    println("\n9. SUMMARY")

    val totalTransactions = transactions.count()

    println(s"Total transactions: $totalTransactions")
    println(s"Valid transactions: ${validTransactions.length}")
    println(s"Bad records: ${badRecords.value}")
    println("Broadcast: Product master reference data")
    println("Accumulator: Counted invalid transactions")
    println("RDD cache: Prevented repeated accumulator updates")

    println("\n============================================================")
    println("DAY 11 COMPLETED SUCCESSFULLY")
    println("============================================================")

    // ------------------------------------------------------------
    // 13. Clean up
    // ------------------------------------------------------------

    broadcastProductMaster.destroy()

    spark.stop()
  }
}
