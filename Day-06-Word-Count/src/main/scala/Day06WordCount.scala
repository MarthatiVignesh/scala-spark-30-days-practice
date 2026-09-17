import org.apache.spark.sql.SparkSession

object Day06WordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 6 - Word Count")
      .master("local[4]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    println("==============================================")
    println("Day 6 - Word Count")
    println("==============================================")

    val logs = sc.textFile("data/application.log")

    println("\nTotal log lines:")
    println(logs.count())

    // Classic Word Count
    val words = logs
      .flatMap(line => line.split("\\s+"))
      .filter(word => word.nonEmpty)

    val classicWordCounts = words
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)

    println("\nClassic Word Count:")
    classicWordCounts
      .collect()
      .sortBy(-_._2)
      .foreach { case (word, count) =>
        println(s"$word -> $count")
      }

    // Case-insensitive Word Count
    val cleanedWords = logs
      .flatMap(line => line.split("\\s+"))
      .map(word => word.replaceAll("[^A-Za-z0-9]", "").toLowerCase)
      .filter(word => word.nonEmpty)

    val wordCounts = cleanedWords
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)

    println("\nCase-Insensitive Word Count:")
    wordCounts
      .collect()
      .sortBy(-_._2)
      .foreach { case (word, count) =>
        println(s"$word -> $count")
      }

    // Top 10 most frequent words
    val top10Words = wordCounts
      .map { case (word, count) => (count, word) }
      .sortByKey(ascending = false)
      .take(10)

    println("\n==============================================")
    println("Top 10 Most Frequent Words")
    println("==============================================")

    top10Words.zipWithIndex.foreach {
      case ((count, word), index) =>
        println(s"${index + 1}. $word -> $count")
    }

    println("\n==============================================")
    println("Word Count Flow")
    println("==============================================")
    println("1. flatMap     -> Split lines into words")
    println("2. map         -> Convert each word to (word, 1)")
    println("3. reduceByKey -> Add counts for each word")

    println("\nLazy transformation operations:")
    println("flatMap, filter, map, reduceByKey, sortByKey")

    println("\nActions:")
    println("count, collect, take")

    println("\nDay 6 completed successfully!")

    spark.stop()
  }
}
