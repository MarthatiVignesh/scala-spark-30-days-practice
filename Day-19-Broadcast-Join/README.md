# Day 19 — Broadcast Join

## Objective

Practice Broadcast Join in Apache Spark by joining a larger orders dataset with a small products dataset.

The implementation also compares a normal inner join with an explicit Broadcast Join and checks the physical execution plan.

## Project Structure

```
Day-19-Broadcast-Join/
├── data/
│   ├── orders.csv
│   └── products.csv
├── project/
│   └── build.properties
├── screenshots/
│   ├── day19-broadcast-join-1.png
│   ├── day19-broadcast-join-2.png
│   └── day19-broadcast-join-3.png
├── src/
│   └── main/
│       └── scala/
│           └── Day19BroadcastJoin.scala
├── build.sbt
└── output.txt
```

## Input Data

### Orders

The orders dataset contains 12 records with:

- order_id
- product_id
- customer_id
- quantity
- amount

### Products

The products dataset contains 6 records with:

- product_id
- product_name
- category

The products table is small compared with the orders table, making it suitable for demonstrating a Broadcast Join.

## Implementation

The Scala program performs:

1. Reads orders and products from CSV files.
2. Performs a normal inner join using `product_id`.
3. Performs an explicit Broadcast Join using Spark's `broadcast()` function.
4. Displays the Broadcast Join physical execution plan.
5. Verifies that both joins return the same number of records.
6. Produces a category-wise order and amount summary.
7. Prints performance notes and final project evidence.

## Broadcast Join

The Broadcast Join is implemented using:

```scala
.join(
  broadcast(products).alias("p"),
  col("o.product_id") === col("p.product_id"),
  "inner"
)
```

The small products dataset is broadcast to the executors. This can reduce the need for a large shuffle of the bigger dataset when one side of the join is sufficiently small.

Broadcasting should be used carefully when the lookup dataset is large because the broadcast data must be distributed to executors.

## Physical Plan Evidence

The executed Spark plan selected:

```
BroadcastHashJoin Inner BuildRight
BroadcastExchange
```

This confirms that Spark used a broadcast-based join for this workload.

## Results

| Metric | Result |
|---|---:|
| Orders | 12 |
| Products | 6 |
| Normal Join Records | 12 |
| Broadcast Join Records | 12 |
| Categories | 2 |

### Category Summary

| Category | Order Count | Total Amount |
|---|---:|---:|
| Electronics | 6 | 236000 |
| Accessories | 6 | 35000 |

## Performance Notes

Broadcast Join is useful when one side of a join is small. The small dataset is broadcast to the executors, which can avoid a large shuffle of the bigger dataset.

The broadcast dataset should be kept within a practical size for executor memory.

## How to Run

From the Day 19 directory:

```bash
sbt compile
sbt run
```

To save the complete execution output:

```bash
sbt run > output.txt 2>&1
```

To inspect the final evidence:

```bash
tail -20 output.txt
```

## Evidence

The `screenshots/` directory contains three screenshots showing:

1. Orders, products, normal join and Broadcast Join results.
2. Broadcast Join physical plan, result verification and category summary.
3. Performance notes and final project evidence.

## Final Status

```
DAY 19 COMPLETED SUCCESSFULLY
```

Day 19 Broadcast Join practice was successfully compiled and executed using Apache Spark 3.5.3 and Scala 2.12.18.
