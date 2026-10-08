import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object Day24StatelessVsStateful {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 24 - Stateless vs Stateful Streaming")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    // Required for stateful DStream operations
    ssc.checkpoint("output/checkpoint")

    val lines = ssc.socketTextStream("localhost", 9999)

    println("======================================================")
    println("      DAY 24 - STATELESS VS STATEFUL STREAMING")
    println("======================================================")
    println("Spark Version  : 3.5.3")
    println("Scala Version  : 2.12.18")
    println("Input Source   : TCP Socket")
    println("Host           : localhost")
    println("Port           : 9999")
    println("Batch Interval : 5 seconds")
    println("======================================================")

    // --------------------------------------------------
    // Parse input
    // --------------------------------------------------

    val transactions: DStream[(String, Int)] = lines.flatMap { line =>

      val parts = line.split(",")

      if (parts.length == 2) {
        try {
          Some((parts(0).trim, parts(1).trim.toInt))
        } catch {
          case _: NumberFormatException => None
        }
      } else {
        None
      }
    }

    // --------------------------------------------------
    // STATELESS PROCESSING
    // Current micro-batch only
    // --------------------------------------------------

    val statelessTotals = transactions.reduceByKey(_ + _)

    // --------------------------------------------------
    // STATEFUL PROCESSING
    // Maintains running totals across micro-batches
    // --------------------------------------------------

    val updateFunction =
      (newValues: Seq[Int], runningTotal: Option[Int]) => {

        val newTotal = newValues.sum

        Some(runningTotal.getOrElse(0) + newTotal)
      }

    val statefulTotals =
      transactions.updateStateByKey[Int](updateFunction)

    // --------------------------------------------------
    // Output
    // --------------------------------------------------

    statelessTotals.foreachRDD { (rdd, time) =>

      if (!rdd.isEmpty()) {

        println()
        println("------------------------------------------------------")
        println(s"Micro-batch Time : $time")
        println("STATELESS RESULT - Current Batch Only")
        println("------------------------------------------------------")

        rdd
          .sortByKey()
          .collect()
          .foreach {
            case (account, amount) =>
              println(f"$account%-10s $amount")
          }
      }
    }

    statefulTotals.foreachRDD { (rdd, time) =>

      if (!rdd.isEmpty()) {

        println()
        println("------------------------------------------------------")
        println(s"Micro-batch Time : $time")
        println("STATEFUL RESULT - Running Total")
        println("------------------------------------------------------")

        rdd
          .sortByKey()
          .collect()
          .foreach {
            case (account, amount) =>
              println(f"$account%-10s $amount")
          }
      }
    }

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

    ssc.awaitTermination()
  }
}
