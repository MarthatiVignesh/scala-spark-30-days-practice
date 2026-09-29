import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day18Joins {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day18Joins")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ------------------------------------------------------------
    // 1. READ INPUT DATASETS
    // ------------------------------------------------------------

    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    val orders = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/orders.csv")

    val payments = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/payments.csv")

    println("\n================ CUSTOMERS ================")
    customers.show(false)

    println("\n================ ORDERS ================")
    orders.show(false)

    println("\n================ PAYMENTS ================")
    payments.show(false)


    // ------------------------------------------------------------
    // 2. INNER JOIN
    // Orders + Customers
    // ------------------------------------------------------------

    println("\n================ INNER JOIN ================")

    val innerJoin = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "inner"
      )
      .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("o.product"),
        col("o.amount")
      )

    innerJoin.show(false)


    // ------------------------------------------------------------
    // 3. LEFT JOIN
    // Orders + Customers
    // ------------------------------------------------------------

    println("\n================ LEFT JOIN ================")

    val leftJoin = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "left"
      )
      .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("o.product"),
        col("o.amount")
      )

    leftJoin.show(false)


    // ------------------------------------------------------------
    // 4. NULL HANDLING AFTER LEFT JOIN
    // ------------------------------------------------------------

    println("\n================ LEFT JOIN + NULL HANDLING ================")

    val leftJoinNullHandled = leftJoin
      .withColumn(
        "customer_name",
        coalesce(col("customer_name"), lit("UNKNOWN CUSTOMER"))
      )
      .withColumn(
        "city",
        coalesce(col("city"), lit("UNKNOWN CITY"))
      )

    leftJoinNullHandled.show(false)


    // ------------------------------------------------------------
    // 5. RIGHT JOIN
    // Orders + Customers
    // ------------------------------------------------------------

    println("\n================ RIGHT JOIN ================")

    val rightJoin = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "right"
      )
      .select(
        col("o.order_id"),
        col("c.customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("o.product"),
        col("o.amount")
      )

    rightJoin.show(false)


    // ------------------------------------------------------------
    // 6. FULL JOIN
    // Orders + Customers
    // ------------------------------------------------------------

    println("\n================ FULL JOIN ================")

    val fullJoin = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "full"
      )
      .select(
        col("o.order_id"),
        coalesce(
          col("o.customer_id"),
          col("c.customer_id")
        ).alias("customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("o.product"),
        col("o.amount")
      )

    fullJoin.show(false)


    // ------------------------------------------------------------
    // 7. ORDERS + PAYMENTS
    // LEFT JOIN WITH NULL HANDLING
    // ------------------------------------------------------------

    println("\n================ ORDERS + PAYMENTS ================")

    val orderPayments = orders.alias("o")
      .join(
        payments.alias("p"),
        col("o.order_id") === col("p.order_id"),
        "left"
      )
      .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("o.product"),
        col("o.amount"),
        col("p.payment_method"),
        col("p.payment_status"),
        col("p.paid_amount")
      )
      .withColumn(
        "payment_status",
        coalesce(col("payment_status"), lit("NOT PAID"))
      )
      .withColumn(
        "payment_method",
        coalesce(col("payment_method"), lit("N/A"))
      )

    orderPayments.show(false)


    // ------------------------------------------------------------
    // 8. THREE-WAY JOIN
    // Orders + Customers + Payments
    // ------------------------------------------------------------

    println("\n================ THREE-WAY JOIN ================")

    val finalJoin = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "left"
      )
      .join(
        payments.alias("p"),
        col("o.order_id") === col("p.order_id"),
        "left"
      )
      .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("c.segment"),
        col("o.product"),
        col("o.amount"),
        coalesce(
          col("p.payment_method"),
          lit("N/A")
        ).alias("payment_method"),
        coalesce(
          col("p.payment_status"),
          lit("NOT PAID")
        ).alias("payment_status"),
        coalesce(
          col("p.paid_amount"),
          lit(0)
        ).alias("paid_amount")
      )

    finalJoin.show(false)


    // ------------------------------------------------------------
    // 9. PHYSICAL PLAN
    // Used to inspect Spark's join execution strategy
    // ------------------------------------------------------------

    println("\n================ PHYSICAL PLAN ================")

    finalJoin.explain("formatted")


    // ------------------------------------------------------------
    // 10. FINAL PROJECT EVIDENCE
    // ------------------------------------------------------------

    println("\n================ FINAL PROJECT EVIDENCE ================")

    println(s"Customers : ${customers.count()}")
    println(s"Orders    : ${orders.count()}")
    println(s"Payments  : ${payments.count()}")

    println(s"Inner Join Records : ${innerJoin.count()}")
    println(s"Left Join Records  : ${leftJoin.count()}")
    println(s"Right Join Records : ${rightJoin.count()}")
    println(s"Full Join Records  : ${fullJoin.count()}")
    println(s"Final Joined Records : ${finalJoin.count()}")

    println("\nDAY 18 COMPLETED SUCCESSFULLY")

    spark.stop()
  }
}
