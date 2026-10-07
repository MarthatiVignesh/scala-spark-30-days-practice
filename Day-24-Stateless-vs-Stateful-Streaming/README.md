# Day 24 — Stateless vs Stateful Streaming

## Objective

Understand the difference between stateless and stateful processing with Spark DStreams and implement both approaches on the same TCP streaming source.

## Requirements Covered

- Create a StreamingContext with a 5-second batch interval.
- Read streaming transaction data from a TCP socket.
- Parse ACCOUNT_ID,AMOUNT records.
- Implement stateless processing with reduceByKey.
- Implement stateful processing with updateStateByKey.
- Configure checkpointing for stateful processing.
- Compare current micro-batch results with running totals across micro-batches.
- Observe shuffle/state-size considerations and explain performance implications.

## Input Format

ACCOUNT_ID,AMOUNT

Example:
A001,100
A002,250
A001,150
A003,300

## Stateless Processing

The stateless branch uses reduceByKey and calculates totals only for the current micro-batch.

Example current batch:
A001,100
A001,150
A002,250

Result:
A001 250
A002 250

A later batch does not automatically include the previous batch's totals.

## Stateful Processing

The stateful branch uses updateStateByKey to maintain a running total for each account across micro-batches.

If one batch contains A001,100 and A001,150 and the next batch contains A001,50, the running state becomes A001 = 300.

Checkpointing is configured at:

output/checkpoint

## How to Run

From this directory:

sbt clean
sbt compile
sbt run

The application listens on TCP port 9999.

In another WSL terminal, if netcat is installed:

nc localhost 9999

Send records such as:

A001,100
A002,250
A001,150

Wait for the 5-second micro-batch output. Send another batch:

A001,50
A002,100

The stateless output shows only the totals from that current batch, while the stateful output retains the totals from earlier batches.

## Processing Flow

TCP Socket
    |
DStream[String]
    |
Parse ACCOUNT_ID,AMOUNT
    |
    +----------------------------+
    |                            |
Stateless                    Stateful
reduceByKey                  updateStateByKey
    |                            |
Current batch total          Running total

## Transformations and Output Operations

- flatMap — parses valid input records.
- map — creates (account, amount) pairs.
- reduceByKey — computes current-batch totals.
- updateStateByKey — updates persistent per-account state.
- foreachRDD — processes each micro-batch.
- isEmpty — skips empty RDDs.
- collect — displays the small demo batch.

collect is used only for this local learning/demo workload. Production streaming applications should avoid collecting large datasets to the driver.

## Performance Notes

- reduceByKey can cause a shuffle because values for the same key must be brought together.
- Stateful processing stores state for active keys, so state size can grow as the number of accounts grows.
- A shorter batch interval can reduce latency but increases scheduling overhead.
- Checkpointing is important for recovering stateful streaming computations.
- Monitor executor memory, state size, shuffle, processing time, and batch scheduling delay.
- DStreams are useful for learning legacy Spark Streaming concepts; newer production applications generally use Structured Streaming where appropriate.

## Expected Observation

| Processing | Result |
|---|---|
| Stateless | Only current micro-batch total |
| Stateful | Running total including previous batches |

Example:

Batch 1: A001,100
Stateless: A001 = 100
Stateful: A001 = 100

Batch 2: A001,50
Stateless: A001 = 50
Stateful: A001 = 150

## Project Structure

Day-24-Stateless-vs-Stateful-Streaming/
├── build.sbt
├── project/
│   └── build.properties
├── data/
│   └── sample_transactions.txt
├── output/
├── screenshots/
├── src/
│   └── main/
│       └── scala/
│           └── Day24StatelessVsStateful.scala
└── README.md

## Conclusion

Day 24 demonstrates the key difference between stateless and stateful DStream processing. Stateless operations work independently on each micro-batch, while stateful processing maintains information across micro-batches using updateStateByKey and checkpointing.
