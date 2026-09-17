import org.apache.spark.sql.SparkSession

object Day08DAGSparkExecution {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 8 - DAG and Spark Execution")
      .master("local[4]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    println("====================================================")
    println("Day 8 - DAG and Spark Execution")
    println("====================================================")

    // Create RDD from sales file
    val sales = sc.textFile("data/sales.txt")

    println("\nInput Sales Data:")
    sales.collect().foreach(println)

    // Narrow Transformation 1: filter
    val validSales = sales
      .filter(line => line.split(",").length == 3)

    // Narrow Transformation 2: map
    val productRevenue = validSales
      .map { line =>
        val parts = line.split(",")
        (parts(0), parts(2).toInt)
      }

    // Wide Transformation: reduceByKey
    val revenueByProduct = productRevenue
      .reduceByKey(_ + _)

    // Action 1
    val result = revenueByProduct.collect().sortBy(-_._2)

    println("\nRevenue By Product:")
    result.foreach {
      case (product, revenue) =>
        println(s"$product -> ₹$revenue")
    }

    // Action 2
    val totalProducts = revenueByProduct.count()

    println("\nNumber of Products:")
    println(totalProducts)

    // Display partitions
    println("\n====================================================")
    println("Partition Information")
    println("====================================================")
    println(s"Input partitions: ${sales.getNumPartitions}")
    println(s"Product revenue partitions: ${productRevenue.getNumPartitions}")
    println(s"Final RDD partitions: ${revenueByProduct.getNumPartitions}")

    // DAG explanation
    println("\n====================================================")
    println("DAG and Execution Flow")
    println("====================================================")

    println("""
sales
  |
  | filter()
  v
validSales
  |
  | map()
  v
productRevenue
  |
  | reduceByKey()
  | SHUFFLE BOUNDARY
  v
revenueByProduct
  |
  | collect()
  v
Final Result
""")

    println("====================================================")
    println("Jobs, Stages, Tasks and Partitions")
    println("====================================================")

    println("Job: Created when an action such as collect() or count() runs.")
    println("Stage: A group of operations separated by shuffle boundaries.")
    println("Task: Work performed on one partition.")
    println("Partition: A logical division of an RDD.")

    println("\nFor this reduceByKey pipeline:")
    println("Stage 0: textFile -> filter -> map")
    println("Stage 1: reduceByKey -> collect")
    println("reduceByKey creates a shuffle boundary.")

    println("\n====================================================")
    println("Narrow vs Wide Transformations")
    println("====================================================")

    println("filter()     -> Narrow transformation")
    println("map()        -> Narrow transformation")
    println("reduceByKey() -> Wide transformation")
    println("collect()    -> Action")
    println("count()      -> Action")

    println("\n====================================================")
    println("DAG Explanation")
    println("====================================================")

    println("Spark builds a Directed Acyclic Graph (DAG) from transformations.")
    println("The DAG represents dependencies between RDDs.")
    println("Narrow transformations can be pipelined within a stage.")
    println("Wide transformations such as reduceByKey require a shuffle.")
    println("Shuffle creates a boundary between stages.")

    println("\n====================================================")
    println("Day 8 completed successfully!")
    println("====================================================")

    spark.stop()
  }
}
