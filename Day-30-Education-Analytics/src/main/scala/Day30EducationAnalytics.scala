import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object Day30EducationAnalytics {

  def main(args: Array[String]): Unit = {

    // ============================================================
    // SPARK SESSION
    // ============================================================

    val spark = SparkSession.builder()
      .appName("Day30-Education-Analytics")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    println()
    println("====================================================")
    println("DAY 30 - EDUCATION ANALYTICS FINAL CAPSTONE")
    println("====================================================")

    // ============================================================
    // 1. READ BATCH DATA
    // ============================================================

    val students = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .csv("data/students.txt")
      .toDF(
        "studentId",
        "studentName",
        "studentType",
        "department"
      )

    val courses = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .csv("data/courses.txt")
      .toDF(
        "courseId",
        "courseName",
        "category"
      )

    val assessments = spark.read
      .option("header", "false")
      .option("inferSchema", "false")
      .csv("data/assessments.txt")
      .toDF(
        "assessmentId",
        "studentId",
        "courseId",
        "assessmentType",
        "score"
      )
      .withColumn(
        "score",
        col("score").cast("double")
      )

    println()
    println("BATCH DATA COUNTS")
    println("-----------------")
    println(s"Students     : ${students.count()}")
    println(s"Courses      : ${courses.count()}")
    println(s"Assessments  : ${assessments.count()}")

    // ============================================================
    // 2. DATA CLEANING + ACCUMULATOR
    // ============================================================

    val invalidAssessmentAccumulator =
      spark.sparkContext.longAccumulator(
        "Invalid Assessment Records"
      )

    val assessmentRDD =
      assessments.rdd.map { row =>

        val score =
          row.getAs[Double]("score")

        if (score < 0 || score > 100) {
          invalidAssessmentAccumulator.add(1)
        }

        row
      }

    val cleanedAssessments =
      assessmentRDD.filter { row =>

        val score =
          row.getAs[Double]("score")

        score >= 0 && score <= 100
      }

    val cleanAssessments =
      spark.createDataFrame(
        cleanedAssessments,
        assessments.schema
      )

    println()
    println("DATA QUALITY")
    println("------------")
    println(
      s"Valid Assessments   : ${cleanAssessments.count()}"
    )
    println(
      s"Invalid Assessments : ${invalidAssessmentAccumulator.value}"
    )

    // ============================================================
    // 3. UDF PERFORMANCE CLASSIFICATION
    // ============================================================

    val performanceUDF =
      udf { score: Double =>

        if (score >= 85)
          "Excellent"
        else if (score >= 70)
          "Good"
        else if (score >= 50)
          "Average"
        else
          "Needs Improvement"
      }

    val classifiedAssessments =
      cleanAssessments.withColumn(
        "performanceLevel",
        performanceUDF(col("score"))
      )

    println()
    println("UDF PERFORMANCE CLASSIFICATION")
    println("------------------------------")

    classifiedAssessments
      .groupBy("performanceLevel")
      .count()
      .orderBy(desc("count"))
      .show(false)

    // ============================================================
    // 4. BROADCAST JOINS
    // ============================================================

    val enriched =
      classifiedAssessments
        .join(
          broadcast(students),
          Seq("studentId"),
          "inner"
        )
        .join(
          broadcast(courses),
          Seq("courseId"),
          "inner"
        )

    println()
    println("BROADCAST ENRICHMENT")
    println("--------------------")
    println(
      s"Enriched Records : ${enriched.count()}"
    )

    enriched.show(false)

    // ============================================================
    // 5. CACHE / PERSIST
    // ============================================================

    val persistedData =
      enriched.persist(
        StorageLevel.MEMORY_ONLY
      )

    // Materialize cache
    persistedData.count()

    println()
    println("CACHE / PERSIST")
    println("---------------")
    println(
      s"Storage Level : ${persistedData.storageLevel}"
    )

    // ============================================================
    // 6. REPARTITIONING
    // ============================================================

    val partitionedData =
      persistedData.repartition(
        4,
        col("studentId")
      )

    println()
    println("PARTITION TUNING")
    println("----------------")
    println(
      s"Student Partitions : ${partitionedData.rdd.getNumPartitions}"
    )

    // ============================================================
    // 7. STUDENT PERFORMANCE AGGREGATION
    // ============================================================

    val studentPerformance =
      partitionedData
        .groupBy(
          "studentId",
          "studentName",
          "department"
        )
        .agg(
          round(
            avg("score"),
            2
          ).as("averageScore"),

          round(
            sum("score"),
            2
          ).as("totalScore"),

          count("*").as(
            "assessmentCount"
          )
        )
        .orderBy(
          desc("averageScore")
        )

    println()
    println("STUDENT PERFORMANCE")
    println("-------------------")

    studentPerformance.show(false)

    // ============================================================
    // 8. PAIR RDD AGGREGATION
    // ============================================================

    val pairRDDTotals =
      partitionedData.rdd
        .map { row =>

          (
            row.getAs[String](
              "studentId"
            ),
            row.getAs[Double](
              "score"
            )
          )
        }
        .reduceByKey(
          (a: Double, b: Double) =>
            a + b
        )

    println()
    println("PAIR RDD TOTAL SCORES")
    println("---------------------")

    pairRDDTotals
      .sortByKey()
      .collect()
      .foreach {

        case (studentId, total) =>

          println(
            f"$studentId%-5s | Total Score: $total%.2f"
          )
      }

    // ============================================================
    // 9. COURSE ANALYTICS
    // ============================================================

    val courseAnalytics =
      partitionedData
        .groupBy(
          "courseId",
          "courseName",
          "category"
        )
        .agg(
          round(
            avg("score"),
            2
          ).as("averageScore"),

          count("*").as(
            "assessmentCount"
          )
        )
        .orderBy(
          desc("averageScore")
        )

    println()
    println("COURSE ANALYTICS")
    println("----------------")

    courseAnalytics.show(false)

    // ============================================================
    // 10. WINDOW FUNCTION
    // ============================================================

    val departmentWindow =
      Window
        .partitionBy(
          "department"
        )
        .orderBy(
          desc("averageScore")
        )

    val rankedStudents =
      studentPerformance.withColumn(
        "departmentRank",
        row_number().over(
          departmentWindow
        )
      )

    println()
    println("DEPARTMENT WINDOW RANKING")
    println("-------------------------")

    rankedStudents.show(false)

    // ============================================================
    // 11. SPARK SQL REPORT
    // ============================================================

    rankedStudents.createOrReplaceTempView(
      "student_performance"
    )

    val sqlReport =
      spark.sql(
        """
          SELECT
            department,
            studentName,
            ROUND(averageScore, 2) AS averageScore,
            assessmentCount,
            departmentRank
          FROM student_performance
          ORDER BY department, departmentRank
        """
      )

    println()
    println("SPARK SQL REPORT")
    println("----------------")

    sqlReport.show(false)

    // ============================================================
    // 12. PHYSICAL PLAN
    // ============================================================

    println()
    println("PHYSICAL PLAN")
    println("-------------")

    sqlReport.explain(true)

    // ============================================================
    // 13. FINAL BATCH SUMMARY
    // ============================================================

    val validAssessmentCount =
      cleanAssessments.count()

    val totalScore =
      cleanAssessments
        .agg(
          sum("score")
        )
        .first()
        .getDouble(0)

    println()
    println("====================================================")
    println("DAY 30 BATCH SUMMARY")
    println("====================================================")

    println(
      s"Valid Assessments    : $validAssessmentCount"
    )

    println(
      f"Total Score          : $totalScore%.2f"
    )

    println(
      s"Student Partitions   : ${partitionedData.rdd.getNumPartitions}"
    )

    println(
      "Broadcast Join       : COMPLETED"
    )

    println(
      "Accumulator          : COMPLETED"
    )

    println(
      "Cache / Persist      : COMPLETED"
    )

    println(
      "Repartitioning       : COMPLETED"
    )

    println(
      "Pair RDD             : COMPLETED"
    )

    println(
      "UDF Classification   : COMPLETED"
    )

    println(
      "Window Function      : COMPLETED"
    )

    println(
      "Spark SQL            : COMPLETED"
    )

    println(
      "Physical Plan        : DISPLAYED"
    )

    // ============================================================
    // 14. STREAMING ATTENDANCE
    // ============================================================

    println()
    println("====================================================")
    println("STREAMING ATTENDANCE COMPONENT")
    println("====================================================")

    val ssc =
      new StreamingContext(
        spark.sparkContext,
        Seconds(5)
      )

    val checkpointDir =
      "output/day30-checkpoint"

    ssc.checkpoint(
      checkpointDir
    )

    val attendanceStream =
      ssc.socketTextStream(
        "localhost",
        9997
      )

    val parsedAttendance:
      DStream[(String, Int)] =
        attendanceStream.flatMap {

          line =>

            val parts =
              line.split(",")

            if (parts.length == 3) {

              val studentId =
                parts(0).trim

              val status =
                parts(2)
                  .trim
                  .toUpperCase

              val presentValue =
                if (
                  status == "PRESENT"
                )
                  1
                else
                  0

              Some(
                studentId ->
                  presentValue
              )

            } else {

              None
            }
        }

    // ============================================================
    // 15. STATEFUL UPDATESTATEBYKEY
    // ============================================================

    val updateState =
      (
        newValues: Seq[Int],
        runningCount: Option[Int]
      ) => {

        val currentCount =
          newValues.sum

        Some(
          runningCount
            .getOrElse(0) +
          currentCount
        )
      }

    val runningAttendance =
      parsedAttendance
        .updateStateByKey[Int](
          updateState
        )

    runningAttendance.foreachRDD {
      rdd =>

        if (!rdd.isEmpty()) {

          println()
          println("RUNNING PRESENT COUNT")
          println("---------------------")

          rdd
            .sortByKey()
            .collect()
            .foreach {

              case (
                    studentId,
                    count
                  ) =>

                println(
                  s"Student: $studentId | Present Count: $count"
                )
            }
        }
    }

    // ============================================================
    // 16. STREAMING WINDOW
    // ============================================================

    val attendanceWindow =
      parsedAttendance
        .reduceByKeyAndWindow(
          (a: Int, b: Int) =>
            a + b,
          Seconds(20),
          Seconds(10)
        )

    attendanceWindow.foreachRDD {
      rdd =>

        if (!rdd.isEmpty()) {

          println()
          println(
            "20-SECOND ATTENDANCE WINDOW"
          )

          println(
            "---------------------------"
          )

          rdd
            .sortByKey()
            .collect()
            .foreach {

              case (
                    studentId,
                    count
                  ) =>

                println(
                  s"Student: $studentId | Window Present Count: $count"
                )
            }
        }
    }

    // ============================================================
    // 17. START STREAMING
    // ============================================================

    println()
    println(
      "STREAMING SERVER READY"
    )

    println(
      "TCP PORT : 9997"
    )

    println(
      "BATCH INTERVAL : 5 seconds"
    )

    println(
      "WINDOW : 20 seconds"
    )

    println(
      "SLIDE : 10 seconds"
    )

    println()
    println(
      "DAY 30 CAPSTONE RUNNING..."
    )

    ssc.start()

    ssc.awaitTermination()
  }
}
