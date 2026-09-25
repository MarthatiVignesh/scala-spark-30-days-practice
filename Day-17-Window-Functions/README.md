# Day 17 — Window Functions

## Objective

Practice Spark SQL window functions using student and customer policy data.

## Topics Covered

- Use `row_number`, `rank`, and `dense_rank`.
- Partition data by course and customer.
- Find the top 3 students per course.
- Calculate the latest policy record per customer.
- Use `lag` and `lead`.
- Practice window-based analysis with Spark DataFrames.

## Scenario

This practice uses two scenarios:

1. Find the top students in each course based on marks.
2. Find the latest policy for each customer and compare policy amounts across records.

## Technologies

- Scala 2.12.18
- Apache Spark 3.5.3
- Spark SQL
- sbt 2.0.7
- Java 17
- Ubuntu WSL2

## Project Structure

```text
Day-17-Window-Functions/
├── data/
│   ├── students.csv
│   └── customer_policies.csv
├── project/
│   └── build.properties
├── screenshots/
│   ├── day17-output-1.png
│   ├── day17-output-2.png
│   └── day17-output-3.png
├── src/main/scala/
│   └── Day17WindowFunctions.scala
├── build.sbt
├── output.txt
└── README.md
```

## 1. Student Window Functions

The student dataset contains students, courses, and marks.

A window is partitioned by `course` and ordered by marks in descending order.

### row_number

`row_number` assigns a unique sequential number within each course.

### rank

`rank` gives the same rank to students with equal marks and leaves gaps after ties.

### dense_rank

`dense_rank` gives the same rank to equal marks without leaving gaps.

## 2. Top 3 Students Per Course

The program filters students using `dense_rank <= 3`.

Because `dense_rank` includes ties, a course can contain more than three students in the resulting output.

## 3. Customer Policy Window Functions

The customer policy dataset contains multiple policy records for some customers.

The window is partitioned by `customer_id` and ordered by `policy_date`.

### Latest Policy Per Customer

A descending policy-date window with `row_number` identifies the latest policy for each customer.

## 4. lag and lead

The program uses:

- `lag` to access the previous policy amount for the same customer.
- `lead` to access the next policy amount for the same customer.

For the first record of a customer, the previous value is `NULL`. For the last record, the next value is `NULL`.

## 5. Execution Results

The successful execution produced:

```text
Total Students  : 12
Total Policies  : 10
Total Courses   : 3
Total Customers : 4

DAY 17 COMPLETED SUCCESSFULLY
```

### Latest Policies

| Customer | Latest Policy | Date |
|---|---|---|
| C001 | P003 | 2026-09-10 |
| C002 | P005 | 2026-07-20 |
| C003 | P008 | 2026-08-25 |
| C004 | P010 | 2026-09-01 |

## 6. Spark Concepts

### Transformations

Examples used include `withColumn`, `filter`, `select`, and `orderBy`. These transformations are evaluated lazily.

### Actions

Examples include `show()` and `count()`. Actions trigger Spark computation.

### Partitions

The application runs in local mode using `master("local[2]")`, providing two local execution threads.

### Shuffle Boundary

Window operations involving partitioning and ordering can require Spark to redistribute and sort data. The partitioning keys used here are `course` and `customer_id`.

### Possible Optimization

- Select only required columns before window processing.
- Avoid unnecessary repartitioning.
- Use appropriate partitioning keys.
- Filter data as early as possible when the business requirement allows it.

## 7. How to Run

From the Day 17 directory:

### Compile

```bash
sbt compile
```

### Run

```bash
sbt run
```

### Save Output

```bash
sbt run > output.txt 2>&1
```

### View Final Output

```bash
tail -20 output.txt
```

## 8. Screenshots

The execution screenshots are stored in the `screenshots/` directory:

- `day17-output-1.png`
- `day17-output-2.png`
- `day17-output-3.png`

## Completion

**Day 17 — Window Functions completed successfully.**