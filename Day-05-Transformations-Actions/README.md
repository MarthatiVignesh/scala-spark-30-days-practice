# Day 5 – Transformations and Actions

## Objective

Practice Apache Spark RDD transformations and actions using Scala.

The main goals of Day 5 are:

* Practice `map`, `filter`, `flatMap`, `distinct`, and `union`
* Practice `count`, `collect`, `first`, `take`, and `reduce`
* Understand transformations versus actions
* Understand lazy evaluation
* Identify narrow and wide transformations
* Understand shuffle boundaries
* Build a simple log analyzer that counts `ERROR` messages

---

## Project Structure

```text
Day-05-Transformations-Actions/
│
├── build.sbt
├── data/
│   └── application.log
├── output.txt
├── project/
│   └── build.properties
├── screenshots/
│   └── day05-output.png
└── src/
    └── main/
        └── scala/
            └── Day05TransformationsActions.scala
```

---

## Execution Environment

* Apache Spark: `3.5.3`
* Scala: `2.12.18`
* Java: `17.0.20`
* SBT: `2.0.7`
* Master: `local[4]`
* Operating System: Ubuntu WSL2

---

# 1. SparkSession and SparkContext

The application starts by creating a `SparkSession`.

```scala
val spark = SparkSession.builder()
  .appName("Day 5 - Transformations and Actions")
  .master("local[4]")
  .getOrCreate()
```

The SparkContext is obtained from the SparkSession:

```scala
val sc = spark.sparkContext
```

### SparkSession

`SparkSession` is the entry point for working with Spark applications.

### SparkContext

`SparkContext` provides access to Spark's core functionality, including RDD creation and processing.

### `local[4]`

```text
local[4]
```

means the application runs locally using four processing threads.

---

# 2. Creating an RDD

The program creates an RDD from a Scala collection:

```scala
val numbers = sc.parallelize(Seq(1, 2, 3, 4, 5, 5), 4)
```

Input:

```text
1, 2, 3, 4, 5, 5
```

The RDD is created using four partitions.

---

# Transformations

A transformation creates a new RDD from an existing RDD.

Transformations are **lazy**.

This means Spark records the transformation but does not immediately execute the computation.

The computation is triggered when an action is called.

The transformations used in Day 5 are:

1. `map`
2. `filter`
3. `flatMap`
4. `distinct`
5. `union`

---

# 3. map Transformation

The `map` transformation applies a function to every element.

Code:

```scala
val doubled = numbers.map(x => x * 2)
```

### Input

```text
1, 2, 3, 4, 5, 5
```

### Processing

```text
1 × 2 = 2
2 × 2 = 4
3 × 2 = 6
4 × 2 = 8
5 × 2 = 10
5 × 2 = 10
```

### Output

```text
2, 4, 6, 8, 10, 10
```

### Key Point

`map` returns one output element for every input element.

---

# 4. filter Transformation

The `filter` transformation keeps only elements that satisfy a condition.

Code:

```scala
val filtered = numbers.filter(x => x >= 4)
```

### Input

```text
1, 2, 3, 4, 5, 5
```

### Condition

```text
x >= 4
```

### Output

```text
4, 5, 5
```

### Key Point

`filter` can reduce the number of elements in an RDD.

---

# 5. flatMap Transformation

`flatMap` applies a function and then flattens the result.

Input sentences:

```text
Apache Spark
Scala Spark
RDD Practice
```

Code:

```scala
val words = sentences.flatMap(line => line.split(" "))
```

### Output

```text
Apache, Spark, Scala, Spark, RDD, Practice
```

### Difference Between map and flatMap

For example:

```scala
map
```

can produce collections inside a collection:

```text
List(
  List("Apache", "Spark"),
  List("Scala", "Spark")
)
```

`flatMap` flattens them:

```text
Apache, Spark, Scala, Spark
```

### Key Point

`flatMap` is commonly useful for splitting lines into words.

---

# 6. distinct Transformation

The `distinct` transformation removes duplicate values.

Code:

```scala
val distinctNumbers = numbers.distinct()
```

### Input

```text
1, 2, 3, 4, 5, 5
```

### Output

```text
1, 2, 3, 4, 5
```

The duplicate `5` is removed.

---

## Shuffle in distinct

`distinct` can require a **shuffle** because Spark may need to move data between partitions to identify duplicate values.

Conceptually:

```text
Partition 1 ─┐
Partition 2 ─┼──> Shuffle ──> Unique Values
Partition 3 ─┤
Partition 4 ─┘
```

Shuffle can be more expensive because data may need to move between partitions.

Therefore, `distinct` is different from simple element-wise transformations such as `map` and `filter`.

---

# 7. union Transformation

The `union` transformation combines two RDDs.

Code:

```scala
val rdd1 = sc.parallelize(Seq(10, 20, 30))
val rdd2 = sc.parallelize(Seq(40, 50, 60))

val combined = rdd1.union(rdd2)
```

### RDD 1

```text
10, 20, 30
```

### RDD 2

```text
40, 50, 60
```

### Output

```text
10, 20, 30, 40, 50, 60
```

### Key Point

`union` combines the elements of two RDDs.

It does not automatically remove duplicates.

---

# Actions

Actions trigger Spark computation and return a result to the driver or perform an output operation.

The actions used in Day 5 are:

1. `count`
2. `collect`
3. `first`
4. `take`
5. `reduce`

---

# 8. count Action

Code:

```scala
numbers.count()
```

### Output

```text
6
```

There are six elements:

```text
1, 2, 3, 4, 5, 5
```

---

# 9. collect Action

Code:

```scala
numbers.collect()
```

`collect()` returns all elements of the RDD to the driver.

For this project, the data is very small, so `collect()` is safe for demonstration.

### Important

For large datasets, avoid unnecessary use of `collect()` because bringing a large amount of data to the driver can cause memory problems.

---

# 10. first Action

Code:

```scala
numbers.first()
```

### Output

```text
1
```

`first()` returns the first element of the RDD.

---

# 11. take Action

Code:

```scala
numbers.take(3)
```

### Output

```text
1, 2, 3
```

`take(3)` returns the first three elements.

---

# 12. reduce Action

Code:

```scala
numbers.reduce((a, b) => a + b)
```

The values are added together:

```text
1 + 2 + 3 + 4 + 5 + 5 = 20
```

### Output

```text
20
```

`reduce` combines the elements using the supplied function.

---

# 13. Transformations vs Actions

| Transformations    | Actions                          |
| ------------------ | -------------------------------- |
| map                | count                            |
| filter             | collect                          |
| flatMap            | first                            |
| distinct           | take                             |
| union              | reduce                           |
| Return another RDD | Return a result / perform output |
| Lazy               | Trigger execution                |

### Simple Explanation

**Transformation:**

```text
Tell Spark what to do.
```

**Action:**

```text
Tell Spark to actually execute the computation.
```

---

# 14. Lazy Evaluation

Spark uses lazy evaluation.

Consider:

```scala
val doubled = numbers.map(x => x * 2)
```

At this point, Spark does not immediately execute the complete computation.

It builds the processing plan.

When an action is called:

```scala
doubled.collect()
```

Spark executes the required computation.

### Flow

```text
RDD
 ↓
Transformation
 ↓
Transformation
 ↓
Transformation
 ↓
Action
 ↓
Spark executes the job
 ↓
Result
```

### Example

```scala
val doubled = numbers.map(x => x * 2)
```

`map` is lazy.

Then:

```scala
doubled.collect()
```

`collect` is an action and triggers execution.

---

# 15. Narrow and Wide Transformations

Spark transformations can be understood in terms of how data moves between partitions.

## Narrow Transformations

A narrow transformation generally processes data without requiring data from multiple parent partitions to be shuffled across the cluster.

Examples used in this project:

* `map`
* `filter`
* `flatMap`

Conceptually:

```text
Partition 1 → Partition 1
Partition 2 → Partition 2
Partition 3 → Partition 3
```

These operations generally avoid shuffle.

---

## Wide Transformations

A wide transformation may require data to move between partitions.

Example used in this project:

```text
distinct
```

Conceptually:

```text
Partition 1 ─┐
Partition 2 ─┼──> Shuffle ──> New partitions
Partition 3 ─┤
Partition 4 ─┘
```

This shuffle can increase processing cost.

---

# 16. Log Analyzer

The Day 5 assignment also includes a simple log analyzer.

The program reads:

```text
data/application.log
```

The log file contains 10 lines.

### Sample Log

```text
INFO Application started
INFO User logged in
ERROR Database connection failed
INFO Processing request
ERROR File not found
WARN Slow response
ERROR Database connection failed
INFO User logged out
INFO Application stopped
ERROR Network timeout
```

---

# 17. Reading the Log File

The program reads the file using:

```scala
val logs = sc.textFile("data/application.log")
```

This creates an RDD containing the log lines.

---

# 18. Counting Log Lines

Code:

```scala
logs.count()
```

### Output

```text
Total log lines: 10
```

The log file contains 10 records.

---

# 19. Filtering ERROR Messages

The program uses `filter`:

```scala
val errorLogs = logs.filter(line => line.startsWith("ERROR"))
```

Only lines beginning with `ERROR` are retained.

### Detected Errors

```text
ERROR Database connection failed
ERROR File not found
ERROR Database connection failed
ERROR Network timeout
```

---

# 20. ERROR Count

The program then counts the filtered RDD:

```scala
val errorCount = errorLogs.count()
```

### Output

```text
Total ERROR messages: 4
```

Therefore:

```text
Total log lines = 10
ERROR messages  = 4
```

---

# 21. Complete Processing Flow

The Day 5 application follows this flow:

```text
Scala Collection
       ↓
Create RDD
       ↓
map
       ↓
filter
       ↓
flatMap
       ↓
distinct
       ↓
union
       ↓
Actions
       ↓
Results
```

The log analyzer follows:

```text
application.log
       ↓
textFile()
       ↓
Log RDD
       ↓
filter(ERROR)
       ↓
count()
       ↓
4 ERROR messages
```

---

# 22. Input and Output

## Input 1 – Numbers

```text
1, 2, 3, 4, 5, 5
```

## Input 2 – Sentences

```text
Apache Spark
Scala Spark
RDD Practice
```

## Input 3 – Log File

```text
data/application.log
```

---

## Processing Results

### map

```text
2, 4, 6, 8, 10, 10
```

### filter

```text
4, 5, 5
```

### flatMap

```text
Apache, Spark, Scala, Spark, RDD, Practice
```

### distinct

```text
1, 2, 3, 4, 5
```

### union

```text
10, 20, 30, 40, 50, 60
```

### count

```text
6
```

### first

```text
1
```

### take(3)

```text
1, 2, 3
```

### reduce

```text
20
```

### Log Analyzer

```text
Total log lines: 10
Total ERROR messages: 4
```

---

# 23. Performance Considerations

## Avoid unnecessary actions

Every action can trigger computation.

Therefore, unnecessary actions should be avoided when processing large datasets.

---

## Avoid unnecessary collect()

`collect()` brings all records to the driver.

It is acceptable for small datasets used for testing and learning.

For large datasets, use alternatives such as:

```scala
take()
```

or write the data to storage.

---

## Shuffle Awareness

`distinct` can cause a shuffle.

Shuffle involves data movement between partitions and can increase execution time and resource usage.

Therefore, shuffle-producing operations should be used carefully when working with large datasets.

---

## Partitioning

The application uses:

```text
local[4]
```

and the main RDD is created using:

```scala
sc.parallelize(Seq(1, 2, 3, 4, 5, 5), 4)
```

This demonstrates partition-based parallel processing.

---

# 24. Program Summary

The program demonstrates the following Spark concepts:

### RDD Operations

* RDD creation
* `map`
* `filter`
* `flatMap`
* `distinct`
* `union`

### Actions

* `count`
* `collect`
* `first`
* `take`
* `reduce`

### Spark Concepts

* Lazy evaluation
* Narrow transformations
* Wide transformations
* Shuffle
* Partitions
* Driver execution
* Local parallel processing

### Practical Scenario

* Log file processing
* ERROR message filtering
* ERROR message counting

---

# 25. Final Output

```text
==============================================
Day 5 - Transformations and Actions
==============================================

Original RDD:
1, 2, 3, 4, 5, 5

map - Doubled values:
2, 4, 6, 8, 10, 10

filter - Values >= 4:
4, 5, 5

flatMap - Words:
Apache, Spark, Scala, Spark, RDD, Practice

distinct - Unique values:
1, 2, 3, 4, 5

union - Combined RDD:
10, 20, 30, 40, 50, 60

Actions:
count  = 6
first  = 1
take(3) = 1, 2, 3
reduce = 20

==============================================
Log Analyzer
==============================================

Total log lines: 10

ERROR messages:
ERROR Database connection failed
ERROR File not found
ERROR Database connection failed
ERROR Network timeout

Total ERROR messages: 4

==============================================
Transformation and Action Summary
==============================================

Transformations:
1. map
2. filter
3. flatMap
4. distinct
5. union

Actions:
1. count
2. collect
3. first
4. take
5. reduce

Day 5 completed successfully!


---

# 26. Conclusion

Day 5 successfully demonstrates how Apache Spark processes data using RDD transformations and actions.

The program shows that transformations such as `map`, `filter`, and `flatMap` are lazy, while actions such as `count`, `collect`, `first`, `take`, and `reduce` trigger computation.

The project also demonstrates `distinct`, which can introduce a shuffle, and a practical log analyzer that identifies and counts ERROR messages.

## Result

Day 5 completed successfully!
