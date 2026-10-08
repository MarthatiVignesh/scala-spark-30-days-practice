import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object Day25WindowOperations {

  def main(args: Array[String]): Unit = {

    // --------------------------------------------------
    // Spark Configuration
    // --------------------------------------------------

    val conf = new SparkConf()
      .setAppName("Day 25 - Window Operations")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.sparkContext.setLogLevel("ERROR")

    // Checkpointing for window/stateful operations
    ssc.checkpoint("output/checkpoint")

    // --------------------------------------------------
    // TCP Streaming Input
    // --------------------------------------------------

    val lines = ssc.socketTextStream("localhost", 9999)

    println("======================================================")
    println("           DAY 25 - WINDOW OPERATIONS")
    println("======================================================")
    println("Spark Version  : 3.5.3")
    println("Scala Version  : 2.12.18")
    println("Input Source   : TCP Socket")
    println("Host           : localhost")
    println("Port           : 9999")
    println("Batch Interval : 5 seconds")
    println("Window Length  : 20 seconds")
    println("Slide Interval : 10 seconds")
    println("======================================================")

    // --------------------------------------------------
    // Parse Input
    // Format: ACCOUNT_ID,AMOUNT
    // --------------------------------------------------

    val transactions: DStream[(String, Int)] = lines.flatMap { line =>

      val parts = line.split(",")

      if (parts.length == 2) {

        try {
          Some(
            (
              parts(0).trim,
              parts(1).trim.toInt
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

    // --------------------------------------------------
    // 1. countByWindow
    //
    // Counts all transactions in the last 20 seconds.
    // Window slides every 10 seconds.
    // --------------------------------------------------

    val totalTransactionCount =
      transactions
        .map(_ => 1)
        .countByWindow(
          Seconds(20),
          Seconds(10)
        )

    // --------------------------------------------------
    // 2. reduceByKeyAndWindow
    //
    // Calculates total transaction amount
    // for each account within the 20-second window.
    // --------------------------------------------------

    val accountTotals =
      transactions.reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Seconds(20),
        Seconds(10)
      )

    // --------------------------------------------------
    // 3. Transaction Count Per Account
    //
    // Counts transactions for every account
    // within the 20-second window.
    // --------------------------------------------------

    val accountTransactionCounts =
      transactions
        .map {
          case (account, _) =>
            (account, 1)
        }
        .reduceByKeyAndWindow(
          (a: Int, b: Int) => a + b,
          Seconds(20),
          Seconds(10)
        )

    // --------------------------------------------------
    // 4. Burst Detection
    //
    // If an account generates 3 or more transactions
    // within 20 seconds, generate an alert.
    // --------------------------------------------------

    val burstAccounts =
      accountTransactionCounts.filter {
        case (_, count) =>
          count >= 3
      }

    // --------------------------------------------------
    // Display Total Transaction Count
    // --------------------------------------------------

    totalTransactionCount.foreachRDD { (rdd, time) =>

      if (!rdd.isEmpty()) {

        println()
        println("------------------------------------------------------")
        println(s"Window Time : $time")
        println("TOTAL TRANSACTIONS IN 20-SECOND WINDOW")
        println("------------------------------------------------------")

        rdd.collect().foreach { count =>
          println(s"Transaction Count : $count")
        }
      }
    }

    // --------------------------------------------------
    // Display Account Totals
    // --------------------------------------------------

    accountTotals.foreachRDD { (rdd, time) =>

      if (!rdd.isEmpty()) {

        println()
        println("------------------------------------------------------")
        println(s"Window Time : $time")
        println("ACCOUNT TOTALS IN 20-SECOND WINDOW")
        println("------------------------------------------------------")

        rdd
          .sortByKey()
          .collect()
          .foreach {
            case (account, total) =>
              println(f"$account%-10s $total")
          }
      }
    }

    // --------------------------------------------------
    // Display Burst Detection
    // --------------------------------------------------

    burstAccounts.foreachRDD { (rdd, time) =>

      if (!rdd.isEmpty()) {

        println()
        println("------------------------------------------------------")
        println(s"Window Time : $time")
        println("BURST DETECTION")
        println("------------------------------------------------------")

        rdd
          .sortByKey()
          .collect()
          .foreach {
            case (account, count) =>
              println(
                s"ALERT: $account generated $count transactions in 20 seconds"
              )
          }
      }
    }

    // --------------------------------------------------
    // Start Streaming
    // --------------------------------------------------

    ssc.start()

    println()
    println("DStream application started...")
    println("Waiting for streaming transactions...")
    println()
    println("Send data in the format:")
    println("ACCOUNT_ID,AMOUNT")
    println()
    println("Example:")
    println("A001,100")
    println("A002,250")
    println("A001,150")
    println()
    println("Window = 20 seconds")
    println("Slide  = 10 seconds")
    println()

    ssc.awaitTermination()
  }
}
