# Day 23 — DStreams Basics

## Objective
Practice Spark Streaming DStreams with a 5-second micro-batch interval.

## Requirements covered
- Create a StreamingContext
- Read a TCP text stream
- Use flatMap
- Use filter
- Use map
- Use reduceByKey
- Count ERROR log messages in each micro-batch
- Demonstrate word counting
- Use foreachRDD for output
- Understand micro-batch processing and shuffle boundaries

## Run

Start a TCP listener:

    nc -lk 9999

Then run:

    sbt run

Send sample logs such as:

    INFO User login
    INFO Page viewed
    ERROR Database connection failed
    INFO User login
    WARN Slow response
    ERROR Network failure
    INFO User logout

The application processes data every 5 seconds. ERROR lines are filtered and aggregated for each micro-batch.

## Performance notes
- Shorter batch intervals reduce latency but increase scheduling overhead.
- Longer intervals reduce scheduling overhead but increase latency.
- reduceByKey introduces a shuffle boundary.
- collect() is used only for this small learning example and should be avoided for large streams.
- Production applications need appropriate monitoring, checkpointing where required, durable sources, and resource controls.

## Stack
- Scala 2.12.18
- Apache Spark 3.5.3
- Java 17
- sbt 2.0.7

Runtime output should be captured locally after verification.
