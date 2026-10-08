import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

object Day26RealTimeHealthcare {

  case class PatientReading(
      patientId: String,
      timestamp: String,
      heartRate: Int,
      temperature: Double,
      spo2: Int
  )

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day26 Real-Time Healthcare")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.sparkContext.setLogLevel("ERROR")

    // Checkpointing for state/window recovery
    ssc.checkpoint("output/checkpoint")

    // Broadcast healthcare threshold reference data
    val thresholds = ssc.sparkContext.broadcast(
      Map(
        "heartRate" -> 120.0,
        "temperature" -> 38.0,
        "spo2" -> 94.0
      )
    )

    // Accumulator for total alerts
    val totalAlerts = ssc.sparkContext.longAccumulator("Total Healthcare Alerts")

    // TCP input stream
    val lines = ssc.socketTextStream(
      "localhost",
      9999,
      StorageLevel.MEMORY_AND_DISK
    )

    // Parse incoming patient records
    val patients = lines
      .flatMap { line =>
        val parts = line.split(",")

        if (parts.length == 5) {
          try {
            Some(
              PatientReading(
                parts(0).trim,
                parts(1).trim,
                parts(2).trim.toInt,
                parts(3).trim.toDouble,
                parts(4).trim.toInt
              )
            )
          } catch {
            case _: NumberFormatException => None
          }
        } else {
          None
        }
      }
      .persist(StorageLevel.MEMORY_ONLY)

    // Detect abnormal patient readings
    val alerts = patients
      .filter { patient =>
        val t = thresholds.value

        patient.heartRate > t("heartRate") ||
        patient.temperature > t("temperature") ||
        patient.spo2 < t("spo2")
      }

    // 20-second window with 10-second slide
    val alertCounts = alerts
      .map(patient => (patient.patientId, 1))
      .reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Seconds(20),
        Seconds(10)
      )

    // Repeated alert detection: 2 or more alerts in the window
    val repeatedAlerts = alertCounts
      .filter { case (_, count) => count >= 2 }

    // Display current alerts
    alerts.foreachRDD { rdd =>

      val alertPatients = rdd.collect()

      if (alertPatients.nonEmpty) {
        println()
        println("==============================================")
        println("       HEALTHCARE ALERT DETECTION")
        println("==============================================")

        alertPatients.foreach { patient =>

          totalAlerts.add(1)

          val reasons = scala.collection.mutable.ArrayBuffer[String]()

          if (patient.heartRate > thresholds.value("heartRate")) {
            reasons += s"High Heart Rate (${patient.heartRate})"
          }

          if (patient.temperature > thresholds.value("temperature")) {
            reasons += s"High Temperature (${patient.temperature})"
          }

          if (patient.spo2 < thresholds.value("spo2")) {
            reasons += s"Low SpO2 (${patient.spo2})"
          }

          println(
            s"Patient: ${patient.patientId} | " +
            s"Time: ${patient.timestamp} | " +
            s"Reason: ${reasons.mkString(", ")}"
          )
        }

        println("----------------------------------------------")
        println(s"Total Alerts Detected: ${totalAlerts.value}")
        println("==============================================")
      }
    }

    // Display repeated alerts from the 20-second window
    repeatedAlerts.foreachRDD { rdd =>

      val repeated = rdd.collect()

      if (repeated.nonEmpty) {
        println()
        println("**********************************************")
        println("        REPEATED ALERT DETECTION")
        println("**********************************************")

        repeated.foreach {
          case (patientId, count) =>
            println(
              s"Patient: $patientId | " +
              s"Alerts in 20-sec Window: $count | " +
              "STATUS: REPEATED ALERT"
            )
        }

        println("**********************************************")
      }
    }

    ssc.start()

    println("==============================================")
    println("DAY 26 - REAL-TIME HEALTHCARE")
    println("TCP Port       : 9999")
    println("Batch Interval : 5 seconds")
    println("Window         : 20 seconds")
    println("Slide          : 10 seconds")
    println("==============================================")
    println("Waiting for patient data...")
    println()

    ssc.awaitTermination()
  }
}
