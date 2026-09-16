import org.apache.spark.sql.SparkSession

object Day05TransformationsActions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 5 - Transformations and Actions")
      .master("local[4]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    println("==============================================")
    println("Day 5 - Transformations and Actions")
    println("==============================================")

    // ------------------------------------------------
    // 1. Create RDD
    // ------------------------------------------------

    val numbers = sc.parallelize(Seq(1, 2, 3, 4, 5, 5), 4)

    println("\nOriginal RDD:")
    println(numbers.collect().mkString(", "))

    // ------------------------------------------------
    // 2. map - Transformation
    // ------------------------------------------------

    val doubled = numbers.map(x => x * 2)

    println("\nmap - Doubled values:")
    println(doubled.collect().mkString(", "))

    // ------------------------------------------------
    // 3. filter - Transformation
    // ------------------------------------------------

    val filtered = numbers.filter(x => x >= 4)

    println("\nfilter - Values >= 4:")
    println(filtered.collect().mkString(", "))

    // ------------------------------------------------
    // 4. flatMap - Transformation
    // ------------------------------------------------

    val sentences = sc.parallelize(
      Seq(
        "Apache Spark",
        "Scala Spark",
        "RDD Practice"
      )
    )

    val words = sentences.flatMap(line => line.split(" "))

    println("\nflatMap - Words:")
    println(words.collect().mkString(", "))

    // ------------------------------------------------
    // 5. distinct - Transformation
    // ------------------------------------------------

    val distinctNumbers = numbers.distinct()

    println("\ndistinct - Unique values:")
    println(distinctNumbers.collect().sorted.mkString(", "))

    // ------------------------------------------------
    // 6. union - Transformation
    // ------------------------------------------------

    val rdd1 = sc.parallelize(Seq(10, 20, 30))
    val rdd2 = sc.parallelize(Seq(40, 50, 60))

    val combined = rdd1.union(rdd2)

    println("\nunion - Combined RDD:")
    println(combined.collect().mkString(", "))

    // ------------------------------------------------
    // 7. Actions
    // ------------------------------------------------

    println("\nActions:")

    println(s"count  = ${numbers.count()}")
    println(s"first  = ${numbers.first()}")
    println(s"take(3) = ${numbers.take(3).mkString(", ")}")
    println(s"reduce = ${numbers.reduce((a, b) => a + b)}")

    // ------------------------------------------------
    // 8. Log Analyzer
    // ------------------------------------------------

    val logs = sc.textFile("data/application.log")

    println("\n==============================================")
    println("Log Analyzer")
    println("==============================================")

    println(s"Total log lines: ${logs.count()}")

    val errorLogs = logs.filter(line => line.startsWith("ERROR"))

    println("\nERROR messages:")
    errorLogs.collect().foreach(println)

    val errorCount = errorLogs.count()

    println(s"\nTotal ERROR messages: $errorCount")

    println("\n==============================================")
    println("Transformation and Action Summary")
    println("==============================================")

    println("Transformations:")
    println("1. map")
    println("2. filter")
    println("3. flatMap")
    println("4. distinct")
    println("5. union")

    println("\nActions:")
    println("1. count")
    println("2. collect")
    println("3. first")
    println("4. take")
    println("5. reduce")

    println("\nLazy transformations are executed when an action is called.")

    println("\nDay 5 completed successfully!")

    spark.stop()
  }
}
