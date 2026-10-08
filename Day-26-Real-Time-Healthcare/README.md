# Day 26 — Real-Time Healthcare Streaming

## Objective

Build a real-time healthcare monitoring application using Apache Spark Streaming.

The application receives patient health readings through TCP and detects abnormal healthcare conditions in real time.

## Technologies

- Scala 2.12.18
- Apache Spark 3.5.3
- Spark Streaming
- sbt 2.0.7
- Java 17
- WSL2 / Ubuntu

## Patient Data Format

Each input record follows:

```
patientId,timestamp,heartRate,temperature,spo2
```

Example:

```
P002,2026-10-08T11:00:01,125,37.2,97
```

## Healthcare Alert Thresholds

The application uses broadcast reference data for the alert thresholds:

| Metric | Alert Condition |
|---|---|
| Heart Rate | > 120 |
| Temperature | > 38 °C |
| SpO2 | < 94 |

## Streaming Configuration

- TCP Port: `9999`
- Batch Interval: `5 seconds`
- Window: `20 seconds`
- Slide: `10 seconds`

## Main Features

### 1. Real-Time Patient Monitoring

Patient readings are received using a TCP socket stream.

### 2. Abnormal Condition Detection

The application checks each reading against the healthcare thresholds.

Examples:

- Heart rate above 120
- Temperature above 38 °C
- SpO2 below 94

### 3. Broadcast Variables

Healthcare threshold reference data is broadcast to the Spark executors so that the same reference values can be efficiently accessed during processing.

### 4. Persistence

Parsed patient data is persisted using:

```
StorageLevel.MEMORY_ONLY
```

### 5. Accumulator

A Spark `LongAccumulator` tracks the total number of detected healthcare alerts.

### 6. Window Operations

Alert counts are calculated using a:

- 20-second window
- 10-second slide

### 7. Repeated Alert Detection

A patient is identified as having a repeated alert when the patient generates at least 2 abnormal readings within the 20-second window.

## Example Result

The test detected abnormal readings for patient P002:

```
Patient: P002 | Time: 2026-10-08T11:50:00 | Reason: High Heart Rate (125)
Patient: P002 | Time: 2026-10-08T11:50:05 | Reason: High Heart Rate (130)
Patient: P002 | Time: 2026-10-08T11:50:10 | Reason: High Heart Rate (128)
```

Total alerts:

```
Total Alerts Detected: 3
```

Repeated alert detection:

```
Patient: P002 | Alerts in 20-sec Window: 3 | STATUS: REPEATED ALERT
```

## Project Structure

```
Day-26-Real-Time-Healthcare/
├── build.sbt
├── data/
│   └── sample_patient_data.txt
├── project/
│   └── build.properties
├── screenshots/
│   └── day26-healthcare-repeated-alert.png
└── src/
    └── main/
        └── scala/
            └── Day26RealTimeHealthcare.scala
```

## How to Run

From the project directory:

```bash
sbt compile
sbt run
```

In another WSL terminal, start the TCP server:

```bash
nc -lk 9999
```

Then send patient readings such as:

```
P002,2026-10-08T11:50:00,125,37.2,97
P002,2026-10-08T11:50:05,130,37.1,97
P002,2026-10-08T11:50:10,128,37.0,96
```

## Performance Concepts Demonstrated

- Broadcast variables reduce repeated distribution of small reference data.
- `MEMORY_ONLY` persistence keeps reusable streaming data in memory.
- Window operations process data over bounded time intervals.
- Accumulators provide a way to track alert metrics.
- Checkpointing supports recovery for streaming state/window processing.

## Status

**DAY 26 COMPLETED SUCCESSFULLY**