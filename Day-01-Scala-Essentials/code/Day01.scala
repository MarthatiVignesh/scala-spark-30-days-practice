object StudentGradeProcessor {

  // 1. val - immutable variable
  val collegeName = "ABC College"

  // 2. var - mutable variable
  var totalStudents = 4

  // 3. lazy val - calculated when first accessed
  lazy val message = "Student Grade Processor Started"

  // 4. Immutable List
  val students = List(
    ("Teju", 85),
    ("Ravi", 72),
    ("Anu", 91),
    ("Kiran", 64)
  )

  // 5. Immutable Vector
  val studentNames = Vector("Teju", "Ravi", "Anu", "Kiran")

  // 6. Immutable Set
  val departments = Set("CSE", "ECE", "CSE", "IT")

  // 7. Immutable Map
  val subjectMarks = Map(
    "Scala" -> 85,
    "Spark" -> 90,
    "Git" -> 80
  )

  // 8. for-comprehension with yield
  val studentGrades = for {
    (name, marks) <- students
  } yield {

    val grade =
      if (marks >= 90) "A"
      else if (marks >= 80) "B"
      else if (marks >= 70) "C"
      else if (marks >= 60) "D"
      else "F"

    (name, marks, grade)
  }

  def main(args: Array[String]): Unit = {

    println("===== Scala Essentials - Day 1 =====")

    println("College: " + collegeName)
    println("Total students: " + totalStudents)

    println(message)

    println("\n--- Students and Grades ---")

    studentGrades.foreach {
      case (name, marks, grade) =>
        println(name + " -> Marks: " + marks + " -> Grade: " + grade)
    }

    println("\n--- Vector ---")
    println(studentNames)

    println("\n--- Set ---")
    println(departments)

    println("\n--- Map ---")
    println(subjectMarks)

    println("\n--- Students who scored 80 or above ---")

    val highScorers = students.filter {
      case (_, marks) => marks >= 80
    }

    highScorers.foreach {
      case (name, marks) =>
        println(name + " -> " + marks)
    }

    println("\n--- Average Marks ---")

    val totalMarks = students.map {
      case (_, marks) => marks
    }.sum

    val averageMarks = totalMarks.toDouble / students.size

    println("Average marks: " + averageMarks)

    println("\n--- Logger Demo ---")

    val studentLogger = new StudentLogger()
    val adminLogger = new AdminLogger()

    studentLogger.log("Student grade processed")
    adminLogger.log("Grade report generated")
  }
}


// 9. Logger trait
trait Logger {
  def log(message: String): Unit
}


// 10. First implementation
class StudentLogger extends Logger {

  override def log(message: String): Unit = {
    println("[STUDENT] " + message)
  }
}


// 11. Second implementation
class AdminLogger extends Logger {

  override def log(message: String): Unit = {
    println("[ADMIN] " + message)
  }
}
