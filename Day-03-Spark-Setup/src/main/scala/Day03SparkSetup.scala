import org.apache.spark.sql.SparkSession

object Day03SparkSetup {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 3 - Spark Setup")
.master("local[4]")
      .getOrCreate()

    val sc = spark.sparkContext

    println("===================================")
    println("DAY 3 - SPARK SETUP")
    println("===================================")

    println(s"Spark Version: ${spark.version}")
    println(s"Application Name: ${sc.appName}")
    println(s"Master: ${sc.master}")
    println(s"Default Parallelism: ${sc.defaultParallelism}")

    println("\n--- Reading Text File ---")

    val filePath = "data/sample.txt"

    val lines = sc.textFile(filePath)

    println("Number of Lines: " + lines.count())

    println("\nFile Contents:")

    lines.collect().foreach(println)

    println("\n--- Spark Architecture ---")

    println("Driver: Coordinates the Spark application.")
    println("Executor: Performs tasks and stores data.")
    println("Cluster Manager: Allocates resources to Spark applications.")

    println("\n--- Application Completed ---")

    spark.stop()
  }
}
