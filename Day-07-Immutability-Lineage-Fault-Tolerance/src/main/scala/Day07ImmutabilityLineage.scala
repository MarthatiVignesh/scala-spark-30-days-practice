import org.apache.spark.sql.SparkSession

object Day07ImmutabilityLineage {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 7 - Immutability, Lineage and Fault Tolerance")
      .master("local[4]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    println("====================================================")
    println("Day 7 - Immutability, Lineage and Fault Tolerance")
    println("====================================================")

    // Original RDD
    val students = sc.textFile("data/students.txt")

    println("\nOriginal RDD:")
    students.collect().foreach(println)

    // Step 1: Filter students with marks >= 70
    val passedStudents = students
      .filter(line => line.split(",")(1).toInt >= 70)

    // Step 2: Convert records into (name, marks)
    val studentMarks = passedStudents
      .map { line =>
        val parts = line.split(",")
        (parts(0), parts(1).toInt)
      }

    // Step 3: Filter students with marks >= 80
    val highScorers = studentMarks
      .filter { case (_, marks) => marks >= 80 }

    // Step 4: Add 5 bonus marks
    val bonusMarks = highScorers
      .map { case (name, marks) => (name, marks + 5) }

    println("\nFinal RDD after transformations:")
    bonusMarks.collect().sortBy(-_._2).foreach {
      case (name, marks) =>
        println(s"$name -> $marks")
    }

    // Action
    val totalStudents = students.count()

    println("\nTotal students:")
    println(totalStudents)

    // Display RDD lineage
    println("\n====================================================")
    println("RDD Lineage")
    println("====================================================")
    println("students")
    println("   |")
    println("   | filter(marks >= 70)")
    println("   v")
    println("passedStudents")
    println("   |")
    println("   | map(name, marks)")
    println("   v")
    println("studentMarks")
    println("   |")
    println("   | filter(marks >= 80)")
    println("   v")
    println("highScorers")
    println("   |")
    println("   | map(add 5 bonus marks)")
    println("   v")
    println("bonusMarks")
    println("   |")
    println("   | collect()")
    println("   v")
    println("Final Result")

    // Spark's actual lineage information
    println("\n====================================================")
    println("Spark RDD Lineage Information")
    println("====================================================")
    println(bonusMarks.toDebugString)

    // Immutability explanation
    println("\n====================================================")
    println("RDD Immutability")
    println("====================================================")
    println("RDDs are immutable.")
    println("Transformations never modify the original RDD.")
    println("Each transformation creates a new RDD.")
    println("Example: students remains unchanged after filter().")

    // Fault tolerance explanation
    println("\n====================================================")
    println("Fault Tolerance")
    println("====================================================")
    println("Spark stores RDD lineage information.")
    println("If a partition is lost, Spark can recompute it")
    println("using the transformations that created that partition.")
    println("This provides fault tolerance without requiring")
    println("every intermediate RDD to be permanently stored.")

    // Transformation types
    println("\n====================================================")
    println("Transformation Types")
    println("====================================================")
    println("filter -> Narrow transformation")
    println("map    -> Narrow transformation")
    println("collect -> Action")
    println("count   -> Action")

    println("\n====================================================")
    println("Day 7 completed successfully!")
    println("====================================================")

    spark.stop()
  }
}
