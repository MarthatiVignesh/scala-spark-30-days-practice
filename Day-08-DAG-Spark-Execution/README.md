# Day 8 — DAG and Spark Execution

## Objective

Understand Spark DAG, jobs, stages, tasks, partitions, and shuffle boundaries using a `reduceByKey()` pipeline.

## Technologies

* Scala 2.12.18
* Apache Spark 3.5.3
* Java 17.0.20
* sbt 2.0.7
* Ubuntu WSL2

## Project Structure

```text
Day-08-DAG-Spark-Execution/
├── data/
│   └── sales.txt
├── screenshots/
│   ├── day08-dag-output-1.png
│   ├── day08-dag-output-2.png
│   └── day08-dag-output-3.png
├── project/
│   └── build.properties
├── src/main/scala/
│   └── Day08DAGSparkExecution.scala
├── build.sbt
├── output.txt
└── README.md
```

## Input Data

```text
Laptop,Electronics,120000
Mobile,Electronics,75000
Laptop,Electronics,90000
Keyboard,Accessories,6000
Mobile,Electronics,50000
Mouse,Accessories,3000
Laptop,Electronics,110000
Keyboard,Accessories,7000
Mouse,Accessories,4000
Mobile,Electronics,65000
```

## DAG Flow

```text
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
  | SHUFFLE
  v
revenueByProduct
  |
  | collect()
  v
Final Result
```

## Revenue By Product

```text
Laptop   -> ₹320000
Mobile   -> ₹190000
Keyboard -> ₹13000
Mouse    -> ₹7000
```

Number of products:

```text
4
```

## Transformations

| Operation       | Type   | Purpose                           |
| --------------- | ------ | --------------------------------- |
| `filter()`      | Narrow | Remove invalid records            |
| `map()`         | Narrow | Create `(product, revenue)` pairs |
| `reduceByKey()` | Wide   | Aggregate revenue by product      |

`reduceByKey()` causes a **shuffle boundary** because data with the same key must be brought together.

## Jobs, Stages, Tasks and Partitions

* **Job:** Created when an action such as `collect()` or `count()` is executed.
* **Stage:** A group of operations separated by shuffle boundaries.
* **Task:** Work performed on one partition.
* **Partition:** A logical division of an RDD.

For the main `reduceByKey()` pipeline:

```text
Stage 0:
textFile -> filter -> map

       |
       | SHUFFLE
       v

Stage 1:
reduceByKey -> collect
```

The input had:

```text
Input partitions: 2
Product revenue partitions: 2
Final RDD partitions: 2
```

## Actions

```text
collect()
count()
```

Actions trigger Spark execution.

## Narrow vs Wide

### Narrow Transformations

```text
filter()
map()
```

These can be pipelined within a stage.

### Wide Transformation

```text
reduceByKey()
```

This requires a shuffle and creates a stage boundary.

## DAG

DAG means **Directed Acyclic Graph**.

Spark builds a DAG from transformations and uses it to determine the execution plan.

```text
Transformations
      |
      v
     DAG
      |
      v
   Stages
      |
      v
    Tasks
      |
      v
 Partitions
```

## Performance

`filter()` and `map()` are narrow transformations and can be executed without a shuffle.

`reduceByKey()` is a wide transformation and introduces a shuffle.

For large datasets, reducing unnecessary shuffles and choosing suitable partition counts can improve performance.

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
```

## Status

**Day 8 — Completed Successfully ✅**
