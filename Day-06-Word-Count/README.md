# Day 6 - Word Count

## Objective

Implement the classic Word Count program using Apache Spark RDDs.

The program also performs case-insensitive word counting, removes punctuation, ignores empty words, and identifies the top 10 most frequent words from application logs.

---

## Topics Covered

* Classic Word Count
* `flatMap`
* `map`
* `reduceByKey`
* Case-insensitive word counting
* Removing punctuation
* Ignoring empty words
* `sortByKey`
* Transformations and actions
* Lazy evaluation
* Shuffle operations
* Top 10 most frequent words from application logs

---

## Project Structure

```text
Day-06-Word-Count/
├── build.sbt
├── data/
│   └── application.log
├── output.txt
├── project/
│   └── build.properties
├── screenshots/
│   ├── day06-word-count-output.png
│   ├── day06-word-count-output-2.png
│   └── day06-word-count-output-3.png
└── src/
    └── main/
        └── scala/
            └── Day06WordCount.scala
```

---

## Input Data

The application log file contains 14 log messages:

```text
INFO Spark application started
INFO Spark processing started
ERROR Spark database connection failed
WARN Spark application response slow
INFO User login successful
ERROR Database connection failed
INFO Spark processing completed
INFO User logout successful
ERROR Spark application timeout
INFO Spark application completed
WARN Database response slow
INFO Spark processing started
ERROR Spark database connection failed
INFO User login successful
```

The program reads this file using:

```scala
val logs = sc.textFile("data/application.log")
```

---

## Classic Word Count

The classic Spark Word Count follows this sequence:

```text
flatMap → map → reduceByKey
```

### 1. flatMap

`flatMap` splits every log line into individual words.

Example:

```text
INFO Spark application started
```

becomes:

```text
INFO
Spark
application
started
```

It is a transformation and is evaluated lazily.

### 2. map

Each word is converted into a key-value pair:

```text
(word, 1)
```

Example:

```text
Spark → (Spark, 1)
INFO  → (INFO, 1)
```

### 3. reduceByKey

`reduceByKey` combines values belonging to the same word.

For example:

```text
(Spark, 1)
(Spark, 1)
(Spark, 1)
```

becomes:

```text
(Spark, 3)
```

The operation adds the counts for each unique key.

---

## Classic Word Count Result

The classic word count preserves the original capitalization.

Important results include:

```text
Spark -> 9
INFO -> 8
ERROR -> 4
application -> 4
connection -> 3
successful -> 3
failed -> 3
User -> 3
processing -> 3
started -> 3
```

Because this version is case-sensitive:

```text
Database
database
```

are treated as different words.

---

## Case-Insensitive Word Count

The second version normalizes every word before counting.

The processing steps are:

```text
Split words
     ↓
Remove punctuation
     ↓
Convert to lowercase
     ↓
Remove empty words
     ↓
map(word → (word, 1))
     ↓
reduceByKey
```

The cleaning operation is:

```scala
word.replaceAll("[^A-Za-z0-9]", "").toLowerCase
```

### Example

Before cleaning:

```text
Database
database
```

After cleaning:

```text
database
database
```

Therefore, both occurrences are counted together.

The result is:

```text
database -> 4
```

---

## Case-Insensitive Word Count Result

The important results are:

```text
spark -> 9
info -> 8
error -> 4
application -> 4
database -> 4
connection -> 3
successful -> 3
failed -> 3
processing -> 3
started -> 3
user -> 3
warn -> 2
slow -> 2
completed -> 2
login -> 2
response -> 2
logout -> 1
timeout -> 1
```

---

## Top 10 Most Frequent Words

The program finds the 10 most frequently occurring words.

The word counts are converted from:

```text
(word, count)
```

to:

```text
(count, word)
```

using:

```scala
.map { case (word, count) => (count, word) }
```

Then the results are sorted in descending order using:

```scala
.sortByKey(ascending = false)
```

Finally:

```scala
.take(10)
```

selects the top 10 results.

### Top 10 Result

```text
1. spark -> 9
2. info -> 8
3. error -> 4
4. application -> 4
5. database -> 4
6. connection -> 3
7. successful -> 3
8. failed -> 3
9. processing -> 3
10. started -> 3
```

---

## Transformations

Transformations create new RDDs from existing RDDs.

They are **lazy**, meaning Spark does not immediately execute them.

The program uses:

### flatMap

```scala
flatMap(line => line.split("\\s+"))
```

Splits log lines into words.

### filter

```scala
filter(word => word.nonEmpty)
```

Removes empty words.

### map

```scala
map(word => (word, 1))
```

Creates key-value pairs.

### reduceByKey

```scala
reduceByKey((a, b) => a + b)
```

Combines counts for the same word.

### sortByKey

```scala
sortByKey(ascending = false)
```

Sorts the word counts in descending order.

---

## Actions

Actions trigger the execution of Spark transformations.

The program uses:

### count

```scala
logs.count()
```

Counts the total number of log lines.

Result:

```text
14
```

### collect

```scala
classicWordCounts.collect()
```

Brings the complete result to the driver.

### take

```scala
.take(10)
```

Returns the first 10 results after sorting.

---

## Lazy Evaluation

Spark transformations are evaluated lazily.

For example:

```scala
val words = logs
  .flatMap(...)
  .filter(...)
```

does not immediately execute the computation.

The computation starts when an action such as:

```scala
count()
collect()
take()
```

is called.

### Lazy Transformations Used

```text
flatMap
filter
map
reduceByKey
sortByKey
```

### Actions Used

```text
count
collect
take
```

---

## Shuffle Boundary

A shuffle occurs when Spark needs to redistribute data between partitions.

### reduceByKey

`reduceByKey` can cause a shuffle because values belonging to the same key may exist in different partitions.

Example:

```text
Partition 1 → (spark, 1)
Partition 2 → (spark, 1)
Partition 3 → (spark, 1)
```

Spark needs to bring values for the same key together before reducing them.

```text
spark → 3
```

### sortByKey

`sortByKey` can also require a shuffle because records need to be distributed appropriately for sorting across partitions.

---

## Performance Considerations

### reduceByKey

`reduceByKey` performs local combining before data is shuffled, which can reduce the amount of data transferred during the shuffle.

### collect

`collect()` brings all results to the driver.

For a small dataset this is acceptable, but for a very large dataset it can cause driver memory problems.

### take

`take(10)` is more appropriate when only a small number of results are required.

---

## Spark Word Count Flow

```text
Application Logs
       |
       v
     textFile
       |
       v
     flatMap
       |
       v
     filter
       |
       v
      map
       |
       v
 (word, 1)
       |
       v
  reduceByKey
       |
       v
 Word Counts
       |
       v
  sortByKey
       |
       v
    take(10)
       |
       v
   Top 10 Words
```

---

## Execution

### Compile the project

```bash
sbt compile
```

### Run the program

```bash
sbt run
```

### Save the output

```bash
sbt run 2>&1 | tee output.txt
```

---

## Execution Result

The program was successfully compiled and executed using:

```text
Scala: 2.12.18
Apache Spark: 3.5.3
Java: 17.0.20
sbt: 2.0.7
Master: local[4]
```

Total log lines:

```text
14
```

Top 10 most frequent words:

```text
1. spark -> 9
2. info -> 8
3. error -> 4
4. application -> 4
5. database -> 4
6. connection -> 3
7. successful -> 3
8. failed -> 3
9. processing -> 3
10. started -> 3
```

Final execution status:

```text
Day 6 completed successfully!
```

---

## Technologies Used

* **Scala 2.12.18**
* **Apache Spark 3.5.3**
* **Spark Core**
* **Spark SQL**
* **Java 17.0.20**
* **sbt 2.0.7**
* **Ubuntu WSL2**

---

## Learning Outcome

After completing Day 6, the following concepts were practiced:

1. Classic Spark Word Count.
2. RDD transformations.
3. RDD actions.
4. `flatMap → map → reduceByKey` processing.
5. Case-insensitive word counting.
6. Removing punctuation.
7. Ignoring empty words.
8. Lazy evaluation.
9. Shuffle boundaries.
10. Sorting word counts.
11. Finding the top 10 most frequent words.
12. Analyzing application logs using Spark.

---

## Status

**Day 6 - Word Count: Completed Successfully ✅**

