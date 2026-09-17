# Day 7 — Immutability, Lineage and Fault Tolerance

## Objective

Practice RDD immutability, multi-step transformations, RDD lineage, and fault tolerance in Apache Spark.

## Technologies

* Scala 2.12.18
* Apache Spark 3.5.3
* Java 17.0.20
* sbt 2.0.7
* Ubuntu WSL2

## Project Structure

```text
Day-07-Immutability-Lineage-Fault-Tolerance/
├── data/
│   └── students.txt
├── screenshots/
│   ├── day07-lineage-output-1.png
│   ├── day07-lineage-output-2.png
│   └── day07-lineage-output-3.png
├── project/
│   └── build.properties
├── src/main/scala/
│   └── Day07ImmutabilityLineage.scala
├── build.sbt
├── output.txt
└── README.md
```

## Input Data

```text
Teju,85
Ravi,72
Anu,91
Kiran,64
Priya,78
Manoj,88
Sneha,95
Arjun,69
Varun,82
Divya,58
```

## Transformation Chain

```text
students
   |
   | filter(marks >= 70)
   v
passedStudents
   |
   | map(name, marks)
   v
studentMarks
   |
   | filter(marks >= 80)
   v
highScorers
   |
   | map(add 5 bonus marks)
   v
bonusMarks
   |
   | collect()
   v
Final Result
```

## RDD Immutability

RDDs are immutable. A transformation does not modify the original RDD. Instead, it creates a new RDD.

Example:

```scala
val passedStudents = students.filter(...)
```

Here, `students` remains unchanged and `passedStudents` is a new RDD.

## RDD Lineage

Lineage is the sequence of transformations used to create an RDD.

Spark lineage observed in this project:

```text
HadoopRDD
   |
   v
students
   |
 filter
   |
   v
passedStudents
   |
 map
   |
   v
studentMarks
   |
 filter
   |
   v
highScorers
   |
 map
   |
   v
bonusMarks
```

Spark's actual lineage was displayed using:

```scala
bonusMarks.toDebugString
```

## Fault Tolerance

Spark stores RDD lineage information. If a partition is lost, Spark can recompute the required data using the transformations that created it.

## Transformations and Actions

### Transformations

```text
filter()
map()
```

Both are **narrow transformations**.

### Actions

```text
collect()
count()
```

Actions trigger Spark execution.

## Final Output

```text
Sneha -> 100
Anu -> 96
Manoj -> 93
Teju -> 90
Varun -> 87

Total students:
10
```

## Commands Used

Compile:

```bash
sbt compile
```

Run:

```bash
sbt run
```

Save output:

```bash
sbt run 2>&1 | tee output.txt



**Day 7 — Completed Successfully ✅**

