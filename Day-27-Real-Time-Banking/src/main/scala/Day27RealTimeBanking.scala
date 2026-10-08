import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.HashPartitioner

object Day27RealTimeBanking {

  case class Transaction(
      transactionId: String,
      accountId: String,
      timestamp: String,
      amount: Double,
      branchId: String,
      transactionType: String
  )

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day27 Real-Time Banking")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.sparkContext.setLogLevel("ERROR")

    // Checkpointing for window/state recovery
    ssc.checkpoint("output/checkpoint")

    // Broadcast branch risk reference data
    val branchRisk = ssc.sparkContext.broadcast(
      Map(
        "B001" -> "LOW",
        "B002" -> "MEDIUM",
        "B003" -> "HIGH"
      )
    )

    // TCP transaction stream
    val lines = ssc.socketTextStream(
      "localhost",
      9998
    )

    // Parse transaction records
    val transactions = lines.flatMap { line =>
      val parts = line.split(",")

      if (parts.length == 6) {
        try {
          Some(
            Transaction(
              parts(0).trim,
              parts(1).trim,
              parts(2).trim,
              parts(3).trim.toDouble,
              parts(4).trim,
              parts(5).trim
            )
          )
        } catch {
          case _: NumberFormatException => None
        }
      } else {
        None
      }
    }

    // -------------------------------------------------------
    // ACCOUNT-WISE TOTAL TRANSACTION AMOUNT
    // -------------------------------------------------------

    val accountTotals = transactions
      .map(t => (t.accountId, t.amount))
      .reduceByKey(
        (a: Double, b: Double) => a + b,
        new HashPartitioner(4)
      )

    accountTotals.foreachRDD { rdd =>

      val totals = rdd.collect().sortBy(_._1)

      if (totals.nonEmpty) {

        println()
        println("==============================================")
        println("       ACCOUNT TRANSACTION TOTALS")
        println("==============================================")

        totals.foreach {
          case (accountId, total) =>
            println(
              f"Account: $accountId | Total Amount: ₹$total%.2f"
            )
        }

        println("==============================================")
      }
    }

    // -------------------------------------------------------
    // 20-SECOND TRANSACTION BURST DETECTION
    // -------------------------------------------------------

    val transactionCounts = transactions
      .map(t => (t.accountId, 1))
      .reduceByKeyAndWindow(
        (a: Int, b: Int) => a + b,
        Seconds(20),
        Seconds(10),
        new HashPartitioner(4)
      )

    val burstAccounts = transactionCounts
      .filter {
        case (_, count) => count >= 3
      }

    burstAccounts.foreachRDD { rdd =>

      val bursts = rdd.collect().sortBy(_._1)

      if (bursts.nonEmpty) {

        println()
        println("**********************************************")
        println("        TRANSACTION BURST DETECTION")
        println("**********************************************")

        bursts.foreach {
          case (accountId, count) =>
            println(
              s"Account: $accountId | " +
              s"Transactions in 20-sec Window: $count | " +
              "STATUS: BURST DETECTED"
            )
        }

        println("**********************************************")
      }
    }

    // -------------------------------------------------------
    // BRANCH RISK INFORMATION
    // -------------------------------------------------------

    transactions.foreachRDD { rdd =>

      val records = rdd.collect()

      if (records.nonEmpty) {

        println()
        println("--------------- BRANCH RISK ----------------")

        records.foreach { transaction =>

          val risk =
            branchRisk.value.getOrElse(
              transaction.branchId,
              "UNKNOWN"
            )

          println(
            s"Transaction: ${transaction.transactionId} | " +
            s"Account: ${transaction.accountId} | " +
            s"Branch: ${transaction.branchId} | " +
            s"Risk: $risk | " +
            s"Type: ${transaction.transactionType}"
          )
        }

        println("---------------------------------------------")
      }
    }

    // -------------------------------------------------------
    // START STREAMING
    // -------------------------------------------------------

    ssc.start()

    println("==============================================")
    println("DAY 27 - REAL-TIME BANKING")
    println("TCP Port       : 9998")
    println("Batch Interval : 5 seconds")
    println("Window         : 20 seconds")
    println("Slide          : 10 seconds")
    println("Partitions     : 4")
    println("==============================================")
    println("Waiting for banking transactions...")
    println()

    ssc.awaitTermination()
  }
}
