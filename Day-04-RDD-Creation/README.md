# Day 4 – RDD Creation

## Objective
Practice creating and manipulating Apache Spark RDDs using Scala.

## Tasks Completed

### RDD from Scala Collection
Created an RDD with 10, 20, 30, 40, 50 using 4 partitions.

### RDD Transformations
Used map, filter, and flatMap transformations.

### RDD from Text File
Read data/customers.txt.

- Customer records: 20
- Input partitions: 4

### Hyderabad Customers
- C001 - John
- C004 - Priya
- C007 - Kiran
- C013 - Manoj
- C019 - Varun

### Sales RDD
Calculated total sales:

Rs.221000.0

Sales RDD partitions: 4

### Partition Inspection
Used mapPartitionsWithIndex to inspect records across partitions.

## Spark Concepts

### Transformations
- map
- filter
- flatMap
- mapPartitionsWithIndex

Transformations are lazy operations.

### Actions
- count
- collect
- reduce

Actions trigger Spark computation.

### Partitions
The application runs using local[4].

RDDs were configured with multiple partitions to demonstrate parallel processing.

## Execution Environment

- Apache Spark 3.5.3
- Scala 2.12.18
- Java 17.0.20
- SBT 2.0.7
- Master local[4]

## Result

Successfully created RDDs from collections and text files, applied transformations and actions, calculated total sales, and inspected RDD partitioning.
