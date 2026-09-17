# Day 09 - Pair RDD

## Objective
Practice Pair RDD operations in Apache Spark.

## Topics Covered
- Key-Value RDD
- reduceByKey()
- groupByKey()
- mapValues()
- Revenue by Product
- Revenue by Department
- Bank Transaction Aggregation
- reduceByKey vs groupByKey
- Transformations and Actions
- Partitions

## Results

### Product Revenue
- Laptop -> ₹320000
- Mobile -> ₹190000
- Monitor -> ₹45000
- Chair -> ₹27000
- Desk -> ₹18000
- Keyboard -> ₹13000
- Mouse -> ₹7000

### Department Revenue
- Electronics -> ₹555000
- Furniture -> ₹45000
- Accessories -> ₹20000

### Bank Net Balance
- A001 -> ₹65000
- A002 -> ₹20000
- A003 -> ₹80000
- A004 -> ₹47000

## Performance
reduceByKey() performs local aggregation before shuffle, so it generally transfers less data.

groupByKey() groups all values and can require more memory and network transfer.

## Partition Information
- Sales RDD: 2 partitions
- Product Revenue RDD: 2 partitions
- Department Revenue RDD: 2 partitions
- Bank Transactions RDD: 2 partitions

## Technologies
- Scala 2.12.18
- Apache Spark 3.5.3
- sbt 2.0.7
- Java 17

## Run
```bash
sbt compile
sbt run

```

## Status
Day 09 - Pair RDD: Completed Successfully ✅
