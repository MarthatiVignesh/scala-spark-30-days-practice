# Day 16 — Aggregations

## Objective

Practice Spark SQL aggregations and generate hospital department revenue metrics.

## Topics Covered

- `count`
- `sum`
- `avg`
- `min`
- `max`
- `groupBy` with multiple columns
- HAVING-like filtering after aggregation
- Department-wise revenue statistics
- Doctor-wise revenue statistics

## Scenario

Hospital Department Revenue Analysis.

The program reads hospital visit and revenue data from `data/hospital_revenue.csv` and uses Spark DataFrame aggregation functions to calculate overall, department-wise, visit-type-wise, and doctor-wise metrics.

## Technologies

- Scala 2.12.18
- Apache Spark 3.5.3
- Spark SQL
- sbt 2.0.7
- Java 17

## Project Structure

```text
Day-16-Aggregations/
├── build.sbt
├── data/
│   └── hospital_revenue.csv
├── output.txt
├── project/
│   └── build.properties
├── screenshots/
│   ├── day16-output-1.png
│   ├── day16-output-2.png
│   ├── day16-output-3.png
│   └── day16-output-4.png
└── src/
    └── main/
        └── scala/
            └── Day16Aggregations.scala
```

## Aggregations Implemented

### 1. Basic Aggregations

The program calculates:

- Patient count
- Total revenue
- Average revenue
- Minimum revenue
- Maximum revenue

### 2. Department-wise Aggregation

The program uses:

```scala
groupBy("department")
```

and calculates count, sum, average, minimum, and maximum revenue for each department.

### 3. Multiple-column Grouping

The program uses:

```scala
groupBy("department", "visit_type")
```

to calculate visit count, total revenue, and average revenue for each department and visit type combination.

### 4. HAVING-like Filtering

After aggregation, departments are filtered using:

```scala
filter(col("total_revenue") > 50000)
```

This returns departments whose total revenue is greater than 50,000.

### 5. Doctor-wise Aggregation

Revenue statistics are also calculated for each doctor.

## Results

### Overall Statistics

| Metric | Result |
|---|---:|
| Total Patients | 15 |
| Total Revenue | 305000 |
| Average Revenue | 20333.33 |
| Minimum Revenue | 3500 |
| Maximum Revenue | 60000 |

### Department-wise Revenue

| Department | Patients | Total Revenue | Average Revenue |
|---|---:|---:|---:|
| Cardiology | 5 | 121500 | 24300.00 |
| Neurology | 4 | 76500 | 19125.00 |
| Orthopedics | 3 | 74500 | 24833.33 |
| Pediatrics | 3 | 32500 | 10833.33 |

### Departments With Total Revenue > 50000

- Cardiology — 121500
- Neurology — 76500
- Orthopedics — 74500

## Run the Project

From the Day 16 directory:

```bash
sbt compile
sbt run
```

To save the execution output:

```bash
sbt run > output.txt 2>&1
```

## Spark Concepts Demonstrated

- DataFrame creation from CSV
- Schema inference
- Aggregation functions
- `groupBy`
- Multiple-column grouping
- Filtering aggregated results
- Sorting aggregated results
- DataFrame actions such as `show()`, `count()`, and `first()`

## Completion

**DAY 16 COMPLETED SUCCESSFULLY**

The program completed successfully with the expected hospital revenue aggregation results.
