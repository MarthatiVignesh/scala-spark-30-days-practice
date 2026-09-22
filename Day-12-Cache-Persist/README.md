# Day 12 - Cache and Persist

## Aim

To understand and demonstrate caching and persistence in Apache Spark by reusing a cleaned transaction dataset across multiple reports.

## Technologies

- Apache Spark 3.5.3
- Scala 2.12.18
- SBT 2.0.7
- Java 17
- Ubuntu WSL2

## Project Structure

```text
Day-12-Cache-Persist/
├── data/
│   └── transactions.txt
├── screenshots/
│   ├── day12-cache-output-1.png
│   ├── day12-cache-output-2.png
│   └── day12-cache-output-3.png
├── project/
│   └── build.properties
├── src/
│   └── main/
│       └── scala/
│           └── Day12CachePersist.scala
├── build.sbt
├── output.txt
└── README.md
```

## Input Data

The transaction dataset contains 18 records.

Each record contains:

```text
Transaction ID, Customer ID, Product, Amount, Status
```

Example:

```text
T001,C001,Laptop,120000,VALID
```

There are 16 valid transactions and 2 invalid transactions.

## Data Cleaning

The program removes invalid records using these conditions:

1. The record must contain 5 fields.
2. Transaction amount must be greater than 0.
3. Transaction status must be VALID.

After cleaning:

```text
Raw transactions: 18
Valid transactions: 16
Invalid transactions: 2
```

## Cache

The cleaned transaction RDD is cached because it is reused by multiple reports.

```scala
cleanedTransactions.cache()
```

The first action materializes the cached RDD:

```scala
val validCount = cleanedTransactions.count()
```

Storage level after caching:

```text
StorageLevel(memory, deserialized, 1 replicas)
```

This represents the MEMORY_ONLY storage level.

## Report 1 - Total Transactions

The first report calculates the total number of valid transactions.

```text
Total valid transactions: 16
```

## Report 2 - Revenue by Product

The second report calculates total revenue for each product.

```text
Laptop     ₹415000.00
Mobile     ₹245000.00
Monitor    ₹143000.00
Keyboard   ₹13000.00
Mouse      ₹10500.00
```

## Report 3 - Revenue by Customer

The third report calculates total revenue for each customer.

```text
C004       ₹250000.00
C002       ₹235000.00
C001       ₹182000.00
C005       ₹98500.00
C003       ₹61000.00
```

## Cache vs Persist

### cache()

`cache()` stores the RDD using the default MEMORY_ONLY storage level.

```scala
cleanedTransactions.cache()
```

### persist()

`persist()` allows a specific storage level to be selected.

Common storage levels used in Spark include:

```text
MEMORY_ONLY
MEMORY_AND_DISK
DISK_ONLY
```

### Storage Levels

- MEMORY_ONLY - Store partitions in memory.
- MEMORY_AND_DISK - Store partitions in memory and use disk when memory is insufficient.
- DISK_ONLY - Store partitions only on disk.

## Why Cache Is Useful

Caching is useful when:

- The same dataset is used by multiple actions.
- The dataset is expensive to calculate.
- Multiple reports reuse the same cleaned dataset.
- Recomputing the dataset would take additional processing time.

In this project, the cleaned transaction dataset is reused by three reports.

## When Caching Can Hurt Performance

Caching can hurt performance when:

1. The dataset is used only once.
2. The dataset is too large for available memory.
3. Caching causes memory pressure and eviction.
4. Recomputing the dataset is cheaper than storing it.
5. Unnecessary cached datasets consume cluster resources.

## Reusing the Cached Dataset

The same `cleanedTransactions` RDD is reused for:

```text
Report 1 -> Total valid transactions
Report 2 -> Revenue by product
Report 3 -> Revenue by customer
```

This demonstrates reuse of a cleaned dataset across multiple actions.

## Unpersist

After the reports are completed, the cached dataset is removed:

```scala
cleanedTransactions.unpersist()
```

After unpersist:

```text
Storage level after unpersist: StorageLevel(1 replicas)
```

This releases the cached storage.

## Important Concepts Learned

- RDD caching
- RDD persistence
- `cache()`
- `persist()`
- `unpersist()`
- Storage levels
- MEMORY_ONLY
- MEMORY_AND_DISK
- DISK_ONLY
- Reusing RDDs across multiple actions
- Data cleaning
- Performance considerations of caching

## How to Run

Compile the project:

```bash
sbt compile
```

Run the project:

```bash
sbt run
```

Save the output:

```bash
sbt run 2>&1 | tee output.txt
```

## Final Result

```text
Raw transactions: 18
Valid transactions: 16
Invalid transactions: 2
Cache: Successful
Three reports: Successful
Unpersist: Successful
```

```text
DAY 12 COMPLETED SUCCESSFULLY
```

## Screenshots

- day12-cache-output-1.png
- day12-cache-output-2.png
- day12-cache-output-3.png

## Status

Day 12 - Spark Cache and Persist: Completed Successfully
