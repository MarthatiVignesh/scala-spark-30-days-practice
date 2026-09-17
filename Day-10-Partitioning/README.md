# Day 10 - Spark Partitioning

## Objective

The objective of Day 10 is to understand and practice partitioning in Apache Spark.

This practice covers:

* Inspecting partition counts
* Understanding data distribution across partitions
* Using repartition()
* Using coalesce()
* Understanding when to increase or decrease partitions
* Using partitionBy() with Pair RDDs
* Optimizing a dataset suffering from too few partitions
* Understanding the difference between repartition() and coalesce()
* Understanding transformations, actions, and lazy evaluation

## Technologies Used

* Apache Spark 3.5.3
* Scala 2.12.18
* Java 17.0.20
* SBT 2.0.7
* Ubuntu 22.04.5 LTS / WSL2
* Spark Master: local[4]

## Project Structure

```text
Day-10-Partitioning/
|
|-- data/
|   `-- sales.txt
|
|-- screenshots/
|   |-- day10-partitioning-output-1.png
|   |-- day10-partitioning-output-2.png
|   `-- day10-partitioning-output-3.png
|
|-- project/
|   `-- build.properties
|
|-- src/
|   `-- main/
|       `-- scala/
|           `-- Day10Partitioning.scala
|
|-- build.sbt
`-- README.md
```

## Dataset

The project uses sales data stored in:

```text
data/sales.txt
```

Each record contains:

```text
Product,Department,Amount
```

Example:

```text
Laptop,Electronics,120000
Mobile,Electronics,75000
Keyboard,Accessories,6000
```

The dataset contains 20 sales records.

## 1. Original Dataset Partitioning

The sales file was loaded using:

```scala
val sales = sc.textFile("data/sales.txt")
```

Results:

```text
Total records: 20
Original partitions: 2

Partition 0 -> 10 records
Partition 1 -> 10 records
```

The input dataset initially contained 2 partitions.

## 2. Inspecting Partitions

Partition distribution was inspected using:

```scala
mapPartitionsWithIndex
```

This allows us to identify the partition ID and the number of records processed by each partition.

Example:

```text
Partition 0 -> 10 records
Partition 1 -> 10 records
```

## 3. Repartition

The dataset was repartitioned using:

```scala
val repartitionedSales = sales.repartition(4)
```

Result:

```text
Partitions after repartition(4): 4

Partition 0 -> 4 records
Partition 1 -> 6 records
Partition 2 -> 6 records
Partition 3 -> 4 records
```

Repartition can increase or decrease the number of partitions.

It performs a shuffle to redistribute data across the partitions.

## 4. Coalesce

The repartitioned dataset was reduced from 4 partitions to 2 partitions using:

```scala
val coalescedSales = repartitionedSales.coalesce(2)
```

Result:

```text
Partitions after coalesce(2): 2

Partition 0 -> 10 records
Partition 1 -> 10 records
```

Coalesce is mainly used to decrease the number of partitions.

It usually avoids a full shuffle when reducing partitions.

## 5. Too Few Partitions Scenario

A dataset with only one partition was created:

```scala
val tooFewPartitions = sc.parallelize(sales.collect(), 1)
```

Result:

```text
Records: 20
Partitions: 1
```

Having too few partitions can reduce parallelism because one partition may become a processing bottleneck.

The dataset was optimized using:

```scala
val optimizedSales = tooFewPartitions.repartition(4)
```

Result:

```text
Optimized partitions: 4
```

Therefore:

```text
Before optimization: 1 partition
After optimization:  4 partitions
```

Increasing partitions allows Spark to process more data in parallel.

## 6. Pair RDD

A Pair RDD was created in the form:

```text
(Product, Amount)
```

Example:

```text
(Laptop,120000)
(Mobile,75000)
(Keyboard,6000)
```

Before partitioning:

```text
Pair RDD partitions before partitionBy: 2
```

## 7. partitionBy()

A HashPartitioner was applied to the Pair RDD:

```scala
val partitionedProductSales =
  productSales.partitionBy(new HashPartitioner(4))
```

Result:

```text
Pair RDD partitions after partitionBy(4): 4
```

Partition distribution:

```text
Partition 0 -> 0 records
Partition 1 -> 7 records
Partition 2 -> 10 records
Partition 3 -> 3 records
```

`partitionBy()` allows a Pair RDD to be explicitly partitioned using a partitioner such as `HashPartitioner`.

## 8. Revenue by Product

After partitioning, product revenue was calculated using:

```scala
val revenueByProduct = partitionedProductSales
  .reduceByKey(_ + _)
```

Final result:

```text
Laptop     -> ₹415000
Mobile     -> ₹245000
Monitor    -> ₹95000
Chair      -> ₹41000
Desk       -> ₹40000
Keyboard   -> ₹21000
Mouse       -> ₹7000
```

## 9. When to Increase Partitions

Partitions should be increased when:

* The dataset is large
* Existing partitions contain too much data
* Some tasks are processing too much data
* More parallelism is required

Example:

```scala
sales.repartition(4)
```

This distributes the dataset across more partitions.

## 10. When to Decrease Partitions

Partitions can be decreased when:

* The dataset becomes smaller
* There are too many small partitions
* Reducing task scheduling overhead is useful

Example:

```scala
sales.coalesce(2)
```

## 11. Repartition vs Coalesce

### repartition()

```scala
sales.repartition(4)
```

Characteristics:

* Can increase or decrease partitions
* Performs a shuffle
* Redistributes data across partitions
* Useful when better data distribution is required

### coalesce()

```scala
sales.coalesce(2)
```

Characteristics:

* Mainly used to decrease partitions
* Usually avoids a full shuffle
* Useful when reducing the number of partitions

## 12. Partitioning Flow

```text
Original Dataset
       |
       v
2 Partitions
       |
       | repartition(4)
       v
4 Partitions
       |
       | coalesce(2)
       v
2 Partitions
```

For the Pair RDD:

```text
Sales Dataset
      |
      v
Pair RDD
(Product, Amount)
      |
      | partitionBy(HashPartitioner(4))
      v
4 Partitions
      |
      | reduceByKey
      v
Revenue by Product
```

## 13. Transformations Used

The following Spark transformations were practiced:

```text
map
repartition
coalesce
mapPartitionsWithIndex
partitionBy
reduceByKey
```

Transformations are lazy operations.

Spark does not immediately execute transformations. Instead, Spark builds an execution plan.

Execution starts when an action is called.

## 14. Actions Used

The following Spark actions were practiced:

```text
count
collect
```

Examples:

```scala
sales.count()
```

and:

```scala
sales.collect()
```

Actions trigger Spark to execute the required transformations.

## 15. Lazy Evaluation

Spark transformations are lazy.

For example:

```scala
val repartitionedSales = sales.repartition(4)
```

does not immediately execute the complete computation.

When an action such as:

```scala
repartitionedSales.count()
```

is called, Spark executes the required computation.

This lazy evaluation allows Spark to optimize the execution plan.

## 16. Key Learnings

The Day 10 practice demonstrated that partitioning is important for Spark performance.

Key points:

1. Partitions divide a dataset into smaller pieces.
2. Spark can process partitions in parallel.
3. Too few partitions can limit parallelism.
4. Too many partitions can increase scheduling overhead.
5. repartition() can increase or decrease partitions.
6. repartition() performs a shuffle.
7. coalesce() is mainly used to decrease partitions.
8. coalesce() usually avoids a full shuffle.
9. partitionBy() can explicitly partition Pair RDDs.
10. Good partitioning can improve resource utilization and processing performance.

## 17. Commands Used

Compile the project:

```bash
sbt compile
```

Run the Spark application:

```bash
sbt run
```

Save the output:

```bash
sbt run 2>&1 | tee output.txt
```

Check the output file:

```bash
ls -lh output.txt
```

## 18. Verification

The application successfully demonstrated:

```text
Original partitions: 2
Repartitioned partitions: 4
Coalesced partitions: 2
Too few partitions: 1
Optimized partitions: 4
Pair RDD before partitionBy: 2
Pair RDD after partitionBy: 4
```

The revenue calculation was also completed successfully.

## 19. Screenshots

The execution screenshots are stored in:

```text
screenshots/
```

Files:

```text
day10-partitioning-output-1.png
day10-partitioning-output-2.png
day10-partitioning-output-3.png
```

## Status

Day 10 - Spark Partitioning: Completed Successfully
