import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day19BroadcastJoin {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day19BroadcastJoin")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ------------------------------------------------------------
    // 1. READ INPUT DATA
    // ------------------------------------------------------------

    val orders = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/orders.csv")

    val products = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/products.csv")

    println("\n================ ORDERS ================")
    orders.show(false)

    println("\n================ PRODUCTS ================")
    products.show(false)


    // ------------------------------------------------------------
    // 2. NORMAL JOIN
    // ------------------------------------------------------------

    println("\n================ NORMAL JOIN ================")

    val normalJoin = orders.alias("o")
      .join(
        products.alias("p"),
        col("o.product_id") === col("p.product_id"),
        "inner"
      )
      .select(
        col("o.order_id"),
        col("o.product_id"),
        col("o.customer_id"),
        col("o.quantity"),
        col("o.amount"),
        col("p.product_name"),
        col("p.category")
      )

    normalJoin.show(false)


    // ------------------------------------------------------------
    // 3. EXPLICIT BROADCAST JOIN
    // ------------------------------------------------------------

    println("\n================ BROADCAST JOIN ================")

    val broadcastJoin = orders.alias("o")
      .join(
        broadcast(products).alias("p"),
        col("o.product_id") === col("p.product_id"),
        "inner"
      )
      .select(
        col("o.order_id"),
        col("o.product_id"),
        col("o.customer_id"),
        col("o.quantity"),
        col("o.amount"),
        col("p.product_name"),
        col("p.category")
      )

    broadcastJoin.show(false)


    // ------------------------------------------------------------
    // 4. BROADCAST JOIN PHYSICAL PLAN
    // ------------------------------------------------------------

    println("\n================ BROADCAST JOIN PHYSICAL PLAN ================")

    broadcastJoin.explain("formatted")


    // ------------------------------------------------------------
    // 5. VERIFY RESULT
    // ------------------------------------------------------------

    println("\n================ RESULT VERIFICATION ================")

    println(s"Total Orders          : ${orders.count()}")
    println(s"Total Products        : ${products.count()}")
    println(s"Normal Join Records   : ${normalJoin.count()}")
    println(s"Broadcast Join Records: ${broadcastJoin.count()}")


    // ------------------------------------------------------------
    // 6. CATEGORY SUMMARY
    // ------------------------------------------------------------

    println("\n================ CATEGORY SUMMARY ================")

    val categorySummary = broadcastJoin
      .groupBy("category")
      .agg(
        count("*").alias("order_count"),
        sum("amount").alias("total_amount")
      )
      .orderBy(col("total_amount").desc)

    categorySummary.show(false)


    // ------------------------------------------------------------
    // 7. PERFORMANCE NOTE
    // ------------------------------------------------------------

    println("\n================ PERFORMANCE NOTE ================")

    println("Broadcast Join is useful when one side of the join is small.")
    println("The small dataset is broadcast to the executors.")
    println("This can avoid a large shuffle of the bigger dataset.")
    println("Use broadcasting carefully when the lookup dataset is too large.")


    // ------------------------------------------------------------
    // 8. FINAL PROJECT EVIDENCE
    // ------------------------------------------------------------

    println("\n================ FINAL PROJECT EVIDENCE ================")

    println(s"Orders                : ${orders.count()}")
    println(s"Products              : ${products.count()}")
    println(s"Normal Join Records   : ${normalJoin.count()}")
    println(s"Broadcast Join Records: ${broadcastJoin.count()}")
    println(s"Categories            : ${categorySummary.count()}")

    println("\nDAY 19 COMPLETED SUCCESSFULLY")

    spark.stop()
  }
}
