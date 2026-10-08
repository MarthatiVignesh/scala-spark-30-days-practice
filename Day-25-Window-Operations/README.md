# Day 25 — Window Operations

## Objective

Implement Spark Streaming window operations using DStreams and demonstrate:

- `countByWindow`
- `reduceByKeyAndWindow`
- 20-second windows
- 10-second slide intervals
- Transaction counts per account
- Burst detection for accounts generating 3 or more transactions within 20 seconds
- Checkpointing for stateful/window processing

## Environment

- Spark: 3.5.3
- Scala: 2.12.18
- sbt: 2.0.7
- Java: 17
- Input: TCP Socket
- Host: localhost
- Port: 9999
- Batch interval: 5 seconds
- Window length: 20 seconds
- Slide interval: 10 seconds

## Input Format

Transactions are sent as:

```text
ACCOUNT_ID,AMOUNT
```

Example:

```text
A001,100
A002,250
A001,150
```

## Implementation

### 1. countByWindow

Counts the total number of transactions arriving within the current 20-second window.

### 2. reduceByKeyAndWindow

Calculates the total transaction amount for each bank account within the 20-second window.

### 3. Account Transaction Counts

Counts how many transactions each account generated within the current window.

### 4. Burst Detection

An alert is generated when an account produces 3 or more transactions within a 20-second window.

Example:

```text
ALERT: A001 generated 3 transactions in 20 seconds
```

## Window Configuration

```text
Batch Interval  = 5 seconds
Window Length   = 20 seconds
Slide Interval  = 10 seconds
```

The 20-second window considers the most recent four 5-second micro-batches, while the 10-second slide causes the window result to be updated every two batches.

## Performance Considerations

- Window operations may require shuffle operations when aggregating by key.
- Larger windows retain more data and can increase memory usage.
- A smaller slide interval produces results more frequently and can increase processing overhead.
- Checkpointing provides recovery support for stateful/window operations.
- `reduceByKeyAndWindow` performs aggregation by key over the defined window.

## Files

```text
Day-25-Window-Operations/
├── build.sbt
├── data/
│   └── sample_transactions.txt
├── project/
│   └── build.properties
├── screenshots/
│   └── day25-window-operations.png
└── src/
    └── main/
        └── scala/
            └── Day25WindowOperations.scala
```

## Execution

Compile:

```bash
sbt compile
```

Start the streaming application:

```bash
sbt run
```

In another terminal, start the TCP server:

```bash
nc -lk 9999
```

Then send transactions using the format:

```text
A001,100
A002,250
A001,150
```

## Result

The application successfully demonstrates:

- 20-second window processing
- 10-second sliding windows
- Total transaction counts
- Per-account transaction totals
- Per-account transaction counts
- Burst detection
- Spark DStream window operations

## Conclusion

Day 25 demonstrates how Spark Streaming processes data over time-based windows. Window operations allow recent streaming events to be aggregated over a defined duration while the slide interval controls how frequently the results are updated.
