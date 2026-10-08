# Day 28 — Real-Time Booking Analytics

## Objective

Build a real-time booking analytics application using Apache Spark Streaming.

The application processes booking and cancellation events, maintains stateful route occupancy, performs window-based analysis, and generates a SQL report.

## Technologies

- Scala 2.12.18
- Apache Spark 3.5.3
- Spark Streaming
- Spark SQL
- sbt 2.0.7
- Java 17
- TCP Socket Streaming

## Input Format

Each booking event follows:

```
bookingId,customerId,eventType,timestamp,routeId,seats
```

Example:

```
B001,C001,BOOK,2026-10-08T13:00:00,R001,2
B002,C002,BOOK,2026-10-08T13:00:01,R001,3
B003,C003,BOOK,2026-10-08T13:00:02,R002,2
B004,C001,CANCEL,2026-10-08T13:00:03,R001,1
B005,C004,BOOK,2026-10-08T13:00:04,R003,4
B006,C005,BOOK,2026-10-08T13:00:05,R002,3
B007,C006,BOOK,2026-10-08T13:00:06,R001,2
```

## Streaming Configuration

- TCP port: 9998
- Batch interval: 5 seconds
- Window duration: 20 seconds
- Window slide: 10 seconds
- Checkpoint directory: `output/checkpoint`

## Route Reference Data

A broadcast variable is used for small route reference data:

| Route | Description |
|---|---|
| R001 | Hyderabad-Bangalore |
| R002 | Hyderabad-Chennai |
| R003 | Hyderabad-Pune |

Broadcasting allows the same small reference data to be efficiently shared with executors.

## Stateful Route Occupancy

The application uses `updateStateByKey` to maintain running seat occupancy.

The event rules are:

- `BOOK` → add seats
- `CANCEL` → subtract seats

For the sample input:

| Route | Occupied Seats |
|---|---:|
| R001 | 6 |
| R002 | 5 |
| R003 | 4 |

R001 is calculated as:

```
2 + 3 - 1 + 2 = 6
```

## 20-Second Window

The application performs route-level window analysis using:

- Window: 20 seconds
- Slide: 10 seconds
- BOOK events add seats.
- CANCEL events subtract seats.

The tested window results were:

| Route | Seats in Window |
|---|---:|
| R001 | 6 |
| R002 | 5 |
| R003 | 4 |

## SQL Report

The current state is converted into a DataFrame and registered as the temporary SQL view:

```
route_occupancy
```

The SQL report displays:

- Route ID
- Route name
- Occupied seats

The tested SQL results were:

```
R001 | Hyderabad-Bangalore | 6
R002 | Hyderabad-Chennai   | 5
R003 | Hyderabad-Pune      | 4
```

## Project Structure

```
Day-28-Real-Time-Booking/
├── build.sbt
├── data/
│   └── sample_bookings.txt
├── project/
│   └── build.properties
├── screenshots/
│   └── day28-real-time-booking.png
└── src/
    └── main/
        └── scala/
            └── Day28RealTimeBooking.scala
```

## How to Run

Start the TCP server in a separate terminal:

```bash
nc -lk 9998
```

Then run the Spark application:

```bash
cd Day-28-Real-Time-Booking
sbt run
```

Send the booking events through the TCP connection.

## Spark Concepts Demonstrated

- DStreams
- Stateful streaming
- `updateStateByKey`
- Checkpointing
- Broadcast variables
- `reduceByKeyAndWindow`
- 20-second windows
- 10-second sliding windows
- Spark SQL
- DataFrame creation
- Micro-batch processing
- BOOK/CANCEL state updates

## Performance Considerations

- Broadcast variables are appropriate for small route reference data.
- `reduceByKeyAndWindow` performs key-based window aggregation.
- Stateful processing maintains information across batches.
- Checkpointing supports recovery for stateful streaming.
- Window operations process data over overlapping time intervals.

## Test Result

The application was successfully compiled and tested with live TCP streaming.

**Expected route occupancy:**

```
R001 = 6
R002 = 5
R003 = 4
```

**Day 28 completed and tested successfully.**
