
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day21SparkCatalog {

  def main(args: Array[String]): Unit = {

    // ------------------------------------------------------------
    // 1. CREATE SPARK SESSION
    // ------------------------------------------------------------

    val spark = SparkSession.builder()
      .appName("Day21SparkCatalog")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // ------------------------------------------------------------
    // 2. READ HOTEL BOOKINGS
    // ------------------------------------------------------------

    println("\n================ READ HOTEL BOOKINGS ================")

    val bookingsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("dateFormat", "yyyy-MM-dd")
      .csv("data/hotel_bookings.csv")

    bookingsDF.show(20, false)

    println(s"\nBooking Records: ${bookingsDF.count()}")

    // ------------------------------------------------------------
    // 3. INSPECT DATAFRAME SCHEMA
    // ------------------------------------------------------------

    println("\n================ DATAFRAME SCHEMA ================")

    bookingsDF.printSchema()

    // ------------------------------------------------------------
    // 4. CREATE TEMPORARY VIEW
    // ------------------------------------------------------------

    println("\n================ CREATE TEMP VIEW ================")

    bookingsDF.createOrReplaceTempView("hotel_bookings_view")

    println("Temporary view created: hotel_bookings_view")

    // ------------------------------------------------------------
    // 5. QUERY TEMPORARY VIEW
    // ------------------------------------------------------------

    println("\n================ QUERY TEMP VIEW ================")

    val confirmedBookings = spark.sql(
      """
        |SELECT
        |    booking_id,
        |    customer_name,
        |    hotel,
        |    city,
        |    room_type,
        |    amount,
        |    status
        |FROM hotel_bookings_view
        |WHERE status = 'Confirmed'
        |ORDER BY amount DESC
        |""".stripMargin
    )

    confirmedBookings.show(20, false)

    println(s"\nConfirmed Bookings: ${confirmedBookings.count()}")

    // ------------------------------------------------------------
    // 6. LIST DATABASES
    // ------------------------------------------------------------

    println("\n================ LIST DATABASES ================")

    spark.sql("SHOW DATABASES").show(false)

    // ------------------------------------------------------------
    // 7. CREATE ANALYTICS DATABASE
    // ------------------------------------------------------------

    println("\n================ CREATE DATABASE ================")

    spark.sql(
      "CREATE DATABASE IF NOT EXISTS hotel_analytics"
    )

    println("Database created/available: hotel_analytics")

    // ------------------------------------------------------------
    // 8. USE DATABASE
    // ------------------------------------------------------------

    println("\n================ USE DATABASE ================")

    spark.sql("USE hotel_analytics")

    println("Current database: hotel_analytics")

    // ------------------------------------------------------------
    // 9. REMOVE OLD TABLE AND OLD WAREHOUSE LOCATION
    // ------------------------------------------------------------

    println("\n================ DROP OLD TABLE ================")

    spark.sql(
      "DROP TABLE IF EXISTS hotel_analytics.bookings"
    )

    val warehouseTablePath =
      "spark-warehouse/hotel_analytics.db/bookings"

    val warehouseTableDir =
      new java.io.File(warehouseTablePath)

    if (warehouseTableDir.exists()) {

      def deleteDirectory(file: java.io.File): Unit = {

        if (file.isDirectory) {

          val files = file.listFiles()

          if (files != null) {
            files.foreach(deleteDirectory)
          }
        }

        file.delete()
      }

      deleteDirectory(warehouseTableDir)
    }

    println(
      "Existing table and warehouse location removed if present."
    )

    // ------------------------------------------------------------
    // 10. CREATE PERMANENT PARQUET TABLE
    // ------------------------------------------------------------

    println("\n================ CREATE TABLE ================")

    spark.sql(
      """
        |CREATE TABLE hotel_analytics.bookings
        |USING PARQUET
        |AS
        |SELECT *
        |FROM hotel_bookings_view
        |""".stripMargin
    )

    println(
      "Permanent table created: hotel_analytics.bookings"
    )

    // ------------------------------------------------------------
    // 11. LIST TABLES
    // ------------------------------------------------------------

    println("\n================ LIST TABLES ================")

    spark.sql("SHOW TABLES").show(false)

    // ------------------------------------------------------------
    // 12. QUERY REGISTERED TABLE
    // ------------------------------------------------------------

    println("\n================ QUERY TABLE ================")

    val tableBookings = spark.sql(
      """
        |SELECT
        |    booking_id,
        |    customer_name,
        |    hotel,
        |    city,
        |    amount,
        |    status
        |FROM hotel_analytics.bookings
        |WHERE status = 'Confirmed'
        |ORDER BY amount DESC
        |""".stripMargin
    )

    tableBookings.show(20, false)

    println(
      s"\nConfirmed Table Records: ${tableBookings.count()}"
    )

    // ------------------------------------------------------------
    // 13. DESCRIBE TABLE
    // ------------------------------------------------------------

    println("\n================ DESCRIBE TABLE ================")

    spark.sql(
      "DESCRIBE hotel_analytics.bookings"
    ).show(false)

    // ------------------------------------------------------------
    // 14. INSPECT TABLE METADATA
    // ------------------------------------------------------------

    println("\n================ TABLE METADATA ================")

    spark.sql(
      "DESCRIBE EXTENDED hotel_analytics.bookings"
    ).show(100, false)

    // ------------------------------------------------------------
    // 15. HOTEL ANALYTICS
    // ------------------------------------------------------------

    println("\n================ HOTEL ANALYTICS ================")

    val hotelAnalytics = spark.sql(
      """
        |SELECT
        |    hotel,
        |    city,
        |    COUNT(*) AS total_bookings,
        |    SUM(amount) AS total_revenue,
        |    ROUND(AVG(amount), 2) AS average_booking_amount
        |FROM hotel_analytics.bookings
        |WHERE status = 'Confirmed'
        |GROUP BY hotel, city
        |ORDER BY total_revenue DESC
        |""".stripMargin
    )

    hotelAnalytics.show(20, false)

    // ------------------------------------------------------------
    // 16. CITY SUMMARY
    // ------------------------------------------------------------

    println("\n================ CITY SUMMARY ================")

    val citySummary = spark.sql(
      """
        |SELECT
        |    city,
        |    COUNT(*) AS confirmed_bookings,
        |    SUM(amount) AS total_revenue
        |FROM hotel_analytics.bookings
        |WHERE status = 'Confirmed'
        |GROUP BY city
        |ORDER BY total_revenue DESC
        |""".stripMargin
    )

    citySummary.show(20, false)

    // ------------------------------------------------------------
    // 17. FINAL PROJECT EVIDENCE
    // ------------------------------------------------------------

    println("\n================ FINAL PROJECT EVIDENCE ================")

    val totalBookings = bookingsDF.count()
    val confirmedCount = confirmedBookings.count()
    val tableRecordCount = tableBookings.count()
    val hotelCount = hotelAnalytics.count()
    val cityCount = citySummary.count()

    println(
      s"Total Booking Records       : $totalBookings"
    )

    println(
      s"Confirmed Bookings          : $confirmedCount"
    )

    println(
      s"Registered Table Records    : $tableRecordCount"
    )

    println(
      s"Hotels in Analytics Summary : $hotelCount"
    )

    println(
      s"Cities in Analytics Summary : $cityCount"
    )

    println("\nCatalog Operations Completed:")

    println("1. Listed databases")
    println("2. Created temporary view")
    println("3. Queried temporary view")
    println("4. Created analytics database")
    println("5. Created permanent table")
    println("6. Listed tables")
    println("7. Queried registered table")
    println("8. Inspected table schema")
    println("9. Inspected table metadata")

    println("\nDAY 21 COMPLETED SUCCESSFULLY")

    // ------------------------------------------------------------
    // 18. STOP SPARK
    // ------------------------------------------------------------

    spark.stop()
  }
}

