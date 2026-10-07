# Day 23 — DStreams Basics

## Objective

Understand and implement Spark Streaming using DStreams.

## Technologies

- Scala 2.12.18
- Apache Spark 3.5.3
- Java 17
- sbt 2.0.7

## Concepts Covered

- StreamingContext
- DStreams
- Micro-batch processing
- TCP socket streaming
- flatMap
- filter
- map
- reduceByKey
- foreachRDD
- ERROR message counting
- Word counting
- Shuffle boundaries
- Performance considerations

## Streaming Flow

TCP Socket
↓
DStream
↓
flatMap
↓
filter
↓
map
↓
reduceByKey
↓
foreachRDD
↓
Micro-batch Output

## Configuration

- Host: localhost
- Port: 9999
- Batch interval: 5 seconds

## Sample Input

INFO User login
INFO Page viewed
ERROR Database connection failed
INFO User login
WARN Slow response
ERROR Network failure
INFO User logout
ERROR Authentication service failed

## Expected ERROR Count

3

## Performance Notes

A shorter batch interval reduces latency but increases scheduling overhead.

A longer batch interval reduces scheduling overhead but increases latency.

reduceByKey can introduce a shuffle boundary.

collect() is used only for this small learning example and should not be used for large production streams.

Production streaming systems should consider checkpointing, fault tolerance, monitoring, resource management, durable sources, and Kafka or another production message broker.

## Result

The application demonstrates DStream micro-batch processing, ERROR message filtering/counting, and word counting.
