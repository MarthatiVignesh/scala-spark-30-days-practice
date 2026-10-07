import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day23DStreamsBasics {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Day 23 - DStreams Basics")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999)

    println("================================================")
    println("        DAY 23 - DSTREAMS BASICS")
    println("================================================")
    println("Spark Version  : 3.5.3")
    println("Scala Version  : 2.12.18")
    println("Input Source   : TCP Socket")
    println("Host           : localhost")
    println("Port           : 9999")
    println("Batch Interval : 5 seconds")
    println("================================================")

    lines.foreachRDD { (rdd, time) =>

      if (!rdd.isEmpty()) {

        // Collect this micro-batch once
        val messages = rdd.collect()

        // Count ERROR messages from the same micro-batch
        val errorCount = messages.count(_.contains("ERROR"))

        println()
        println("------------------------------------------------")
        println(s"Micro-batch time : $time")
        println(s"Messages received: ${messages.length}")
        println(s"ERROR count      : $errorCount")
        println("------------------------------------------------")

        println()
        println("Word counts:")

        messages
          .flatMap(_.split("\\s+"))
          .filter(_.nonEmpty)
          .map(_.toLowerCase)
          .groupBy(identity)
           
          .mapValues(_.length)
          .toSeq
          .sortBy(_._1)
          .foreach {
            case (word, count) =>
              println(f"$word%-20s $count")
          }

        println("------------------------------------------------")
      }
    }

    ssc.start()

    println()
    println("DStream application started...")
    println("Waiting for streaming data...")
    println("Send log messages to localhost:9999")
    println()

    ssc.awaitTermination()
  }
}
