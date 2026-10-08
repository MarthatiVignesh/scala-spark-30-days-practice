# Day 27 — Real-Time Banking

## Objective

Build a real-time banking transaction monitoring application using Apache Spark Streaming.

The application:
- Processes banking transactions from a TCP socket.
- Calculates running transaction totals by account.
- Detects transaction bursts within a 20-second window.
- Uses a broadcast variable for branch risk information.
- Uses `HashPartitioner(4)` for account-based processing.
- Demonstrates checkpointing and windowed processing.

## Technologies

- Scala 2.12.18
- Apache Spark 3.5.3
- Spark Streaming
- sbt 2.0.7
- Java 17
- TCP Socket Streaming

## Input Format

Each transaction follows:

```
transactionId,accountId,timestamp,amount,branchId,transactionType
```

Example:

```
T001,A001,2026-10-08T12:00:00,5000,B001,DEBIT
T002,A002,2026-10-08T12:00:01,250,B002,CREDIT
T003,A001,2026-10-08T12:00:02,3500,B001,DEBIT
T004,A003,2026-10-08T12:00:03,15000,B003,DEBIT
T005,A001,2026-10-08T12:00:04,5200,B001,DEBIT
T006,A002,2026-10-08T12:00:05,300,B002,DEBIT
```

## Streaming Configuration

- Batch interval: 5 seconds
- Socket port: 9998
- Window duration: 20 seconds
- Window slide: 10 seconds
- Checkpoint directory: `output/checkpoint`

## Branch Risk Reference

A broadcast variable stores the branch risk information:

| Branch | Risk |
|---|---|
| B001 | LOW |
| B002 | MEDIUM |
| B003 | HIGH |

Broadcasting avoids repeatedly shipping the same small reference data to executors.

## Account Transaction Totals

Transactions are mapped by `accountId` and aggregated using `reduceByKey`.

A `HashPartitioner(4)` is used for account-based partitioning.

For the sample input, the expected totals are:

| Account | Total Amount |
|---|---:|
| A001 | ₹13,700 |
| A002 | ₹550 |
| A003 | ₹15,000 |

## Transaction Burst Detection

The application uses a 20-second window with a 10-second slide.

An account is considered to have a transaction burst when it has **3 or more transactions** within the window.

For the sample data:

```
Account: A001
Transactions in 20-sec Window: 3
STATUS: BURST DETECTED
```

## Sample Test Result

The completed test produced:

```
ACCOUNT TRANSACTION TOTALS
Account: A001 | Total Amount: ₹13700.00
Account: A002 | Total Amount: ₹550.00
Account: A003 | Total Amount: ₹15000.00

BRANCH RISK
T001 A001 B001 LOW DEBIT
T002 A002 B002 MEDIUM CREDIT
T003 A001 B001 LOW DEBIT
T004 A003 B003 HIGH DEBIT
T005 A001 B001 LOW DEBIT
T006 A002 B002 MEDIUM DEBIT

TRANSACTION BURST DETECTION
Account: A001 | Transactions in 20-sec Window: 3 | STATUS: BURST DETECTED
```

## Project Structure

```
Day-27-Real-Time-Banking/
├── build.sbt
├── data/
│   └── sample_transactions.txt
├── project/
│   └── build.properties
├── screenshots/
│   └── day27-banking-monitoring.png
└── src/
    └── main/
        └── scala/
            └── Day27RealTimeBanking.scala
```

## How to Run

Start a TCP server:

```bash
nc -lk 9998
```

In another terminal:

```cd Day-27-Real-Time-Banking
sbt run
```

Then send transaction data through the TCP connection.

## Spark Concepts Demonstrated

- DStreams
- Stateless transformations
- Windowed transformations
- `reduceByKey`
- `reduceByKeyAndWindow`
- `HashPartitioner(4)`
- Broadcast variables
- Checkpointing
- Micro-batch processing
- Key-based aggregation
- Burst detection

## Performance Considerations

- `HashPartitioner(4)` distributes account keys across partitions.
- `reduceByKey` performs local aggregation before data is shuffled.
- Broadcast variables are suitable for small reference datasets such as branch risk information.
- Window operations maintain data across overlapping time intervals.
- Checkpointing supports recovery for stateful/windowed streaming workloads.

## Status

**Day 27 completed and tested successfully.**
