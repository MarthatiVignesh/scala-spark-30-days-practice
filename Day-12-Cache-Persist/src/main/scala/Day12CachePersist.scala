import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Day12CachePersist {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day-12-Cache-Persist")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("==================================================")
    println("DAY 12 - CACHE AND PERSIST")
    println("==================================================")

    // Read raw transaction data
    val rawTransactions = spark.sparkContext.textFile("data/transactions.txt")

    println(s"Total raw transactions: ${rawTransactions.count()}")
    println(s"Raw partitions: ${rawTransactions.getNumPartitions}")

    // Clean the transaction data
    val cleanedTransactions = rawTransactions
      .map(_.split(","))
      .filter(parts =>
        parts.length == 5 &&
        parts(3).toDouble > 0 &&
        parts(4) == "VALID"
      )
      .map(parts => (
        parts(0), // Transaction ID
        parts(1), // Customer ID
        parts(2), // Product
        parts(3).toDouble // Amount
      ))

    println()
    println("--------------------------------------------------")
    println("CLEANED TRANSACTION DATA")
    println("--------------------------------------------------")

    println(s"Storage level BEFORE cache: ${cleanedTransactions.getStorageLevel}")

    // Cache the cleaned dataset because it is reused by multiple reports
    cleanedTransactions.cache()

    // First action materializes the cache
    val validCount = cleanedTransactions.count()

    println(s"Valid transactions: $validCount")
    println(s"Storage level AFTER cache: ${cleanedTransactions.getStorageLevel}")

    println()
    println("==================================================")
    println("REPORT 1 - TOTAL TRANSACTIONS")
    println("==================================================")

    val totalTransactions = cleanedTransactions.count()
    println(s"Total valid transactions: $totalTransactions")

    println()
    println("==================================================")
    println("REPORT 2 - REVENUE BY PRODUCT")
    println("==================================================")

    val revenueByProduct = cleanedTransactions
      .map {
        case (_, _, product, amount) => (product, amount)
      }
      .reduceByKey(_ + _)
      .collect()
      .sortBy(-_._2)

    revenueByProduct.foreach {
      case (product, revenue) =>
        println(f"$product%-10s ₹$revenue%.2f")
    }

    println()
    println("==================================================")
    println("REPORT 3 - REVENUE BY CUSTOMER")
    println("==================================================")

    val revenueByCustomer = cleanedTransactions
      .map {
        case (_, customer, _, amount) => (customer, amount)
      }
      .reduceByKey(_ + _)
      .collect()
      .sortBy(-_._2)

    revenueByCustomer.foreach {
      case (customer, revenue) =>
        println(f"$customer%-10s ₹$revenue%.2f")
    }

    println()
    println("==================================================")
    println("CACHE VS PERSIST")
    println("==================================================")

    println("cache() uses MEMORY_ONLY storage level.")
    println("persist() allows choosing a specific storage level.")
    println(s"Current cached storage level: ${cleanedTransactions.getStorageLevel}")

    println()
    println("Available storage levels:")
    println("MEMORY_ONLY       -> Store partitions in memory.")
    println("MEMORY_AND_DISK   -> Memory first, disk if needed.")
    println("DISK_ONLY         -> Store partitions only on disk.")

    println()
    println("==================================================")
    println("WHEN CACHING CAN HURT PERFORMANCE")
    println("==================================================")

    println("1. Dataset is used only once.")
    println("2. Dataset is too large for available memory.")
    println("3. Caching causes memory pressure and eviction.")
    println("4. Recomputing the dataset is cheaper than storing it.")
    println("5. Unnecessary cached datasets consume cluster resources.")

    println()
    println("==================================================")
    println("THREE REPORTS USING THE SAME CACHED DATASET")
    println("==================================================")

    println("Report 1: Total valid transactions")
    println("Report 2: Revenue by product")
    println("Report 3: Revenue by customer")
    println("All three reports reuse cleanedTransactions.")

    println()
    println("==================================================")
    println("UNCACHE DATASET")
    println("==================================================")

    cleanedTransactions.unpersist()

    println(s"Storage level after unpersist: ${cleanedTransactions.getStorageLevel}")

    println()
    println("==================================================")
    println("DAY 12 COMPLETED SUCCESSFULLY")
    println("==================================================")

    spark.stop()
  }
}
