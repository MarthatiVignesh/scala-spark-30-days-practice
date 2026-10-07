import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Day23DStreamsBasics {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Day 23 - DStreams Basics")
      .setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines
      .flatMap(_.split("\\s+"))
      .filter(_.nonEmpty)

    val errorCounts = lines
      .filter(_.contains("ERROR"))
      .map(_ => ("ERROR", 1))
      .reduceByKey(_ + _)

    val wordCounts = words
      .map(_.toLowerCase)
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    println("================================================")
    println("        DAY 23 - DSTREAMS BASICS")
    println("================================================")
    println("Source         : TCP socket localhost:9999")
    println("Batch interval : 5 seconds")
    println("Processing     : ERROR count + word count")
    println("================================================")

    errorCounts.foreachRDD { (rdd, time) =>
      val count = rdd.map(_._2).sum().toInt
      println(s"[$time] ERROR count in current micro-batch: $count")
    }

    wordCounts.foreachRDD { (rdd, time) =>
      if (!rdd.isEmpty()) {
        println(s"[$time] Word counts:")
        rdd.collect().sortBy(_._1).foreach(println)
      }
    }

    ssc.start()
    println("DStream application started. Send log lines to port 9999.")
    ssc.awaitTermination()
  }
}
