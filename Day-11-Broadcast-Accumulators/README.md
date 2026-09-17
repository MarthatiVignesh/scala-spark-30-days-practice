# Day 11 - Broadcast and Accumulators

## Aim

To understand and implement:

* Broadcast variables
* Accumulators
* RDD processing with broadcast data
* Counting bad records
* Why normal driver variables should not be used for distributed updates

## Technologies Used

* Scala 2.12.18
* Apache Spark 3.5.3
* SBT 2.0.7
* Java 17
* Ubuntu WSL2

## Project Structure

```text
Day-11-Broadcast-Accumulators/
│
├── data/
│   ├── product_master.txt
│   └── transactions.txt
│
├── screenshots/
│   ├── day11-broadcast-accumulator-output-1.png
│   └── day11-broadcast-accumulator-output-2.png
│
├── project/
│   └── build.properties
│
├── src/
│   └── main/
│       └── scala/
│           └── Day11BroadcastAccumulators.scala
│
├── build.sbt
└── output.txt
```

## 1. Product Master

The product master contains small reference data.

```text
P001,Laptop,Electronics
P002,Mobile,Electronics
P003,Keyboard,Accessories
P004,Mouse,Accessories
P005,Monitor,Electronics
```

## 2. Transactions

The transaction file contains 10 transactions.

Two transactions contain invalid product IDs:

```text
P999
P888
```

Therefore:

* Total transactions = 10
* Valid transactions = 8
* Invalid transactions = 2

## 3. Broadcast Variable

A broadcast variable is used to send a small piece of data to all executors.

In this project, the product master map is broadcast.

```scala
val broadcastProductMaster = sc.broadcast(productMaster)
```

This allows Spark tasks to use the product reference data efficiently.

## 4. Accumulator

An accumulator is used to count bad records.

```scala
val badRecords = sc.longAccumulator("Bad Records")
```

When an invalid product is found:

```scala
badRecords.add(1)
```

The final result is:

```text
Bad records found: 2
```

## 5. Transaction Validation

Each transaction is checked against the broadcast product master.

Valid example:

```text
VALID,T001,P001,Laptop,Electronics,₹120000.0
```

Invalid example:

```text
INVALID,T004,P999,Unknown Product,Unknown,₹5000.0
```

## 6. Revenue by Department

The valid transactions are grouped by department.

Output:

```text
Accessories     ₹16000.00
Electronics     ₹380000.00
```

## 7. Why Not Use a Normal Driver Variable?

A normal variable belongs to the driver program.

Spark executors process data separately.

Therefore, changes made to a normal driver variable inside distributed tasks are not reliable.

Accumulators are designed for distributed counters.

## 8. RDD Cache

The validated RDD is cached:

```scala
validatedTransactions.cache()
```

This prevents Spark from unnecessarily recomputing the RDD when it is used by multiple actions.

It also prevents the accumulator from being incremented again during recomputation in this example.

## 9. Final Result

```text
Total transactions: 10
Valid transactions: 8
Bad records: 2
Broadcast: Product master reference data
Accumulator: Counted invalid transactions
RDD cache: Prevented repeated accumulator updates
```

## 10. Important Concepts Learned

### Broadcast

Used for sharing small read-only data with executors.

### Accumulator

Used for counters and monitoring information from distributed tasks.

### Cache

Used to keep an RDD in memory so it does not need to be recomputed repeatedly.

### Driver Variable

Normal driver variables should not be used for collecting updates from distributed Spark tasks.

## 11. How to Run

Open the Day 11 directory:

```bash
cd ~/scala-spark-30-day-practice/Day-11-Broadcast-Accumulators
```

Compile:

```bash
sbt compile
```

Run:

```bash
sbt run
```

Save the output:

```bash
sbt run 2>&1 | tee output.txt
```

## 12. Screenshots

The project contains two screenshots showing the Spark execution and final results.

* `day11-broadcast-accumulator-output-1.png`
* `day11-broadcast-accumulator-output-2.png`

## Status

**Day 11 - Broadcast and Accumulators: Completed Successfully**
