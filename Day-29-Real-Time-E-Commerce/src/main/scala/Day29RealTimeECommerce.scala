import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD

object Day29RealTimeECommerce {

  def main(args: Array[String]): Unit = {

    // ------------------------------------------------------------
    // SPARK SESSION
    // ------------------------------------------------------------

    val spark = SparkSession.builder()
      .appName("Day29 Real-Time E-Commerce Analytics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // ------------------------------------------------------------
    // FILE PATHS
    // ------------------------------------------------------------

    val basePath = "data/"

    val ordersPath =
      basePath + "orders.txt"

    val productsPath =
      basePath + "products.txt"

    val customersPath =
      basePath + "customers.txt"

    // ------------------------------------------------------------
    // SCHEMAS
    // ------------------------------------------------------------

    val orderSchema = StructType(
      Seq(
        StructField("orderId", StringType, false),
        StructField("customerId", StringType, false),
        StructField("productId", StringType, false),
        StructField("timestamp", StringType, false),
        StructField("quantity", IntegerType, false),
        StructField("amount", DoubleType, false)
      )
    )

    val productSchema = StructType(
      Seq(
        StructField("productId", StringType, false),
        StructField("productName", StringType, false),
        StructField("category", StringType, false)
      )
    )

    val customerSchema = StructType(
      Seq(
        StructField("customerId", StringType, false),
        StructField("customerName", StringType, false),
        StructField("segment", StringType, false)
      )
    )

    // ------------------------------------------------------------
    // RAW DATA
    // ------------------------------------------------------------

    val rawOrders =
      spark.read
        .schema(orderSchema)
        .option("header", "false")
        .csv(ordersPath)

    val products =
      spark.read
        .schema(productSchema)
        .option("header", "false")
        .csv(productsPath)

    val customers =
      spark.read
        .schema(customerSchema)
        .option("header", "false")
        .csv(customersPath)

    println()
    println("==============================================")
    println("DAY 29 REAL-TIME E-COMMERCE ANALYTICS")
    println("==============================================")

    println()
    println("RAW DATA")
    println(s"Orders    : ${rawOrders.count()}")
    println(s"Products  : ${products.count()}")
    println(s"Customers : ${customers.count()}")

    // ------------------------------------------------------------
    // CLEANING
    // ------------------------------------------------------------

    val cleanOrders =
      rawOrders
        .filter(
          col("orderId").isNotNull &&
          col("customerId").isNotNull &&
          col("productId").isNotNull &&
          col("quantity") > 0 &&
          col("amount") > 0
        )
        .withColumn(
          "eventTime",
          to_timestamp(
            col("timestamp")
          )
        )

    println()
    println("CLEAN DATA")
    println(
      s"Valid Orders: ${cleanOrders.count()}"
    )

    // ------------------------------------------------------------
    // UDF
    // ------------------------------------------------------------

    val classifyCategory =
      udf { category: String =>

        if (category == null) {
          "Unknown"
        } else {

          category.toLowerCase match {

            case "electronics" =>
              "Technology"

            case "accessories" =>
              "Lifestyle"

            case _ =>
              "Other"
          }
        }
      }

    // ------------------------------------------------------------
    // ENRICHMENT
    // ------------------------------------------------------------

    val enriched =
      cleanOrders
        .join(
          products,
          Seq("productId"),
          "inner"
        )
        .join(
          customers,
          Seq("customerId"),
          "inner"
        )
        .withColumn(
          "businessCategory",
          classifyCategory(
            col("category")
          )
        )

    println()
    println("ENRICHED DATA")

    enriched
      .select(
        "orderId",
        "customerId",
        "productId",
        "productName",
        "category",
        "businessCategory",
        "customerName",
        "segment",
        "quantity",
        "amount"
      )
      .show(false)

    // ------------------------------------------------------------
    // REPARTITION BY CUSTOMER
    // ------------------------------------------------------------

    val customerPartitioned =
      enriched.repartition(
        4,
        col("customerId")
      )

    println()
    println("PARTITIONING")
    println(
      s"Partitions after customer repartition: ${customerPartitioned.rdd.getNumPartitions}"
    )

    // ------------------------------------------------------------
    // CUSTOMER REVENUE AGGREGATION
    // ------------------------------------------------------------

    val customerRevenue =
      customerPartitioned
        .groupBy(
          "customerId",
          "customerName"
        )
        .agg(
          sum("amount")
            .alias("totalRevenue"),
          sum("quantity")
            .alias("totalQuantity"),
          count("orderId")
            .alias("orderCount")
        )
        .orderBy(
          desc("totalRevenue")
        )

    println()
    println("CUSTOMER REVENUE")

    customerRevenue.show(false)

    // ------------------------------------------------------------
    // PAIR RDD
    // ------------------------------------------------------------

    val customerPairRDD:
        RDD[(String, Double)] =

      enriched
        .select(
          "customerId",
          "amount"
        )
        .as[(String, Double)]
        .rdd
        .map {
          case (customerId, amount) =>
            (customerId, amount)
        }

    val pairRDDTotals =
      customerPairRDD
        .reduceByKey(
          (a, b) => a + b
        )

    println()
    println("PAIR RDD CUSTOMER TOTALS")

    pairRDDTotals
      .collect()
      .sortBy(-_._2)
      .foreach {
        case (customerId, total) =>
          println(
            f"$customerId%-5s | Total Revenue: ₹$total%.2f"
          )
      }

    // ------------------------------------------------------------
    // CATEGORY AGGREGATION
    // ------------------------------------------------------------

    val categoryRevenue =
      enriched
        .groupBy(
          "businessCategory"
        )
        .agg(
          sum("amount")
            .alias("totalRevenue")
        )
        .orderBy(
          desc("totalRevenue")
        )

    println()
    println("CATEGORY REVENUE")

    categoryRevenue.show(false)

    // ------------------------------------------------------------
    // WINDOW RANKING
    // ------------------------------------------------------------

    val rankingWindow =
      Window
        .partitionBy("businessCategory")
        .orderBy(
          desc("amount")
        )

    val rankedOrders =
      enriched
        .withColumn(
          "categoryRank",
          row_number()
            .over(rankingWindow)
        )

    println()
    println("WINDOW RANKING")

    rankedOrders
      .select(
        "orderId",
        "productName",
        "businessCategory",
        "amount",
        "categoryRank"
      )
      .orderBy(
        "businessCategory",
        "categoryRank"
      )
      .show(false)

    // ------------------------------------------------------------
    // PHYSICAL PLAN
    // ------------------------------------------------------------

    println()
    println("PHYSICAL PLAN")

    customerRevenue.explain(true)

    // ------------------------------------------------------------
    // FINAL SUMMARY
    // ------------------------------------------------------------

    val totalRevenue =
      enriched
        .agg(
          sum("amount")
        )
        .first()
        .getDouble(0)

    val totalOrders =
      enriched.count()

    println()
    println("==============================================")
    println("FINAL E-COMMERCE SUMMARY")
    println("==============================================")
    println(
      s"Valid Enriched Orders : $totalOrders"
    )
    println(
      f"Total Revenue         : ₹$totalRevenue%.2f"
    )
    println(
      s"Customer Partitions   : ${customerPartitioned.rdd.getNumPartitions}"
    )
    println(
      "Pair RDD Aggregation  : COMPLETED"
    )
    println(
      "UDF Classification    : COMPLETED"
    )
    println(
      "Window Ranking        : COMPLETED"
    )
    println(
      "Physical Plan         : DISPLAYED"
    )
    println(
      "DAY 29 COMPLETED SUCCESSFULLY"
    )
    println("==============================================")

    spark.stop()
  }
}
