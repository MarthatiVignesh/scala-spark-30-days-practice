import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

object Day28RealTimeBooking {

  // Booking event schema
  case class Booking(
      bookingId: String,
      customerId: String,
      eventType: String,
      timestamp: String,
      routeId: String,
      seats: Int
  )

  def main(args: Array[String]): Unit = {

    // ------------------------------------------------------------
    // SPARK CONFIGURATION
    // ------------------------------------------------------------

    val conf = new SparkConf()
      .setAppName("Day28 Real-Time Booking")
      .setMaster("local[*]")

    val ssc = new StreamingContext(
      conf,
      Seconds(5)
    )

    ssc.sparkContext.setLogLevel("ERROR")

    // Checkpointing is required for stateful processing.
    ssc.checkpoint("output/checkpoint")

    // ------------------------------------------------------------
    // ROUTE REFERENCE DATA
    // ------------------------------------------------------------

    val routeReference = Map(
      "R001" -> "Hyderabad-Bangalore",
      "R002" -> "Hyderabad-Chennai",
      "R003" -> "Hyderabad-Pune"
    )

    // Broadcast small reference data.
    val broadcastRoutes =
      ssc.sparkContext.broadcast(routeReference)

    // ------------------------------------------------------------
    // TCP STREAM
    // ------------------------------------------------------------

    val lines = ssc.socketTextStream(
      "localhost",
      9998,
      StorageLevel.MEMORY_ONLY
    )

    // ------------------------------------------------------------
    // PARSE BOOKING EVENTS
    // ------------------------------------------------------------

    val bookings = lines.flatMap { line =>

      val parts = line.split(",")

      if (parts.length == 6) {

        try {

          Some(
            Booking(
              bookingId = parts(0),
              customerId = parts(1),
              eventType = parts(2),
              timestamp = parts(3),
              routeId = parts(4),
              seats = parts(5).toInt
            )
          )

        } catch {

          case _: NumberFormatException =>
            None
        }

      } else {

        None
      }
    }

    // ------------------------------------------------------------
    // CONVERT BOOK / CANCEL INTO SEAT CHANGES
    // ------------------------------------------------------------

    val routeSeatChanges = bookings.map { booking =>

      val change =
        booking.eventType.toUpperCase match {

          case "BOOK" =>
            booking.seats

          case "CANCEL" =>
            -booking.seats

          case _ =>
            0
        }

      (booking.routeId, change)
    }

    // ------------------------------------------------------------
    // STATEFUL ROUTE OCCUPANCY
    // ------------------------------------------------------------

    def updateState(
        newValues: Seq[Int],
        currentState: Option[Int]
    ): Option[Int] = {

      val previousSeats =
        currentState.getOrElse(0)

      val currentChange =
        newValues.sum

      Some(
        previousSeats + currentChange
      )
    }

    val routeOccupancy =
      routeSeatChanges.updateStateByKey[Int](
        updateState(_, _)
      )

    // ------------------------------------------------------------
    // DISPLAY CURRENT ROUTE OCCUPANCY
    // ------------------------------------------------------------

    routeOccupancy.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("========== CURRENT ROUTE OCCUPANCY ==========")

        rdd
          .sortByKey()
          .collect()
          .foreach {

            case (routeId, seats) =>

              val routeName =
                broadcastRoutes.value
                  .getOrElse(
                    routeId,
                    "Unknown Route"
                  )

              println(
                f"$routeId%-5s | $routeName%-25s | Seats: $seats"
              )
          }
      }
    }

    // ------------------------------------------------------------
    // 20-SECOND WINDOW
    // SLIDE = 10 SECONDS
    // ------------------------------------------------------------

    val windowBookings =
      bookings.map { booking =>

        val change =
          booking.eventType.toUpperCase match {

            case "BOOK" =>
              booking.seats

            case "CANCEL" =>
              -booking.seats

            case _ =>
              0
          }

        (booking.routeId, change)
      }

    val windowOccupancy =
      windowBookings.reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Seconds(20),
        Seconds(10)
      )

    // ------------------------------------------------------------
    // DISPLAY WINDOW RESULTS
    // ------------------------------------------------------------

    windowOccupancy.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        println()
        println("========== 20-SECOND ROUTE WINDOW ==========")

        rdd
          .sortByKey()
          .collect()
          .foreach {

            case (routeId, seats) =>

              val routeName =
                broadcastRoutes.value
                  .getOrElse(
                    routeId,
                    "Unknown Route"
                  )

              println(
                f"$routeId%-5s | $routeName%-25s | Seats in Window: $seats"
              )
          }
      }
    }

    // ------------------------------------------------------------
    // SQL REPORT
    // ------------------------------------------------------------

    routeOccupancy.foreachRDD { rdd =>

      if (!rdd.isEmpty()) {

        val spark =
          org.apache.spark.sql.SparkSession
            .builder()
            .config(rdd.sparkContext.getConf)
            .getOrCreate()

        import spark.implicits._

        val routeData =
          rdd.map {

            case (routeId, seats) =>

              (
                routeId,
                broadcastRoutes.value
                  .getOrElse(
                    routeId,
                    "Unknown Route"
                  ),
                seats
              )
          }

        val df =
          routeData.toDF(
            "routeId",
            "routeName",
            "occupiedSeats"
          )

        df.createOrReplaceTempView(
          "route_occupancy"
        )

        println()
        println("========== SQL ROUTE REPORT ==========")

        spark
          .sql(
            """
              |SELECT
              |  routeId,
              |  routeName,
              |  occupiedSeats
              |FROM route_occupancy
              |ORDER BY occupiedSeats DESC
            """.stripMargin
          )
          .show(false)
      }
    }

    // ------------------------------------------------------------
    // APPLICATION INFORMATION
    // ------------------------------------------------------------

    println()
    println("==============================================")
    println("DAY 28 REAL-TIME BOOKING ANALYTICS")
    println("TCP Port       : 9998")
    println("Batch Interval : 5 seconds")
    println("Window         : 20 seconds")
    println("Slide          : 10 seconds")
    println("Stateful       : updateStateByKey")
    println("Broadcast      : Route reference data")
    println("Checkpoint     : output/checkpoint")
    println("==============================================")

    // ------------------------------------------------------------
    // START STREAMING
    // ------------------------------------------------------------

    ssc.start()

    ssc.awaitTermination()
  }
}
