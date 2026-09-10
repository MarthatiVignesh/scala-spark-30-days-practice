# Day 3 - Spark Setup and First Application

## Objective

Learn the basic setup and execution of an Apache Spark application using Scala.

## Environment

- Apache Spark: 3.5.3
- Scala: 2.12.18
- Java: 17
- Build Tool: sbt 2.0.7

## Tasks Completed

1. Created a Scala Spark project using sbt.
2. Created SparkSession.
3. Created SparkContext.
4. Read a text file using Spark.
5. Displayed the file contents.
6. Explained Driver, Executor and Cluster Manager.
7. Tested the application using `local[2]`.
8. Tested the application using `local[4]`.

## Spark Architecture

### Driver

The Driver coordinates the Spark application and creates the SparkContext/SparkSession.

### Executor

Executors perform the tasks assigned by the Driver and process data.

### Cluster Manager

The Cluster Manager is responsible for allocating resources to Spark applications.

## local[2] Test

```text
Master: local[2]
Default Parallelism: 2
Number of Lines: 5
Status: SUCCESS
