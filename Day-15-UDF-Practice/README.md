# Day 15 — UDF Practice

## Objective

- Create a Scala UDF to classify customer transaction risk.
- Add calculated columns using `withColumn`.
- Compare a UDF with a built-in Spark function.
- Register a UDF with the Spark session/catalog.
- Build a customer risk category pipeline from transaction values.

## Technologies

- Apache Spark 3.5.3
- Scala 2.12.18
- SBT 2.0.7
- Java 17.0.20
- Ubuntu WSL2

## Project Structure

```text
Day-15-UDF-Practice/
├── data/
│   └── transactions.csv
├── screenshots/
│   ├── day15-output-1.png
│   ├── day15-output-2.png
│   ├── day15-output-3.png
│   └── day15-output-4.png
├── project/
│   └── build.properties
├── src/
│   └── main/
│       └── scala/
│           └── Day15UDFPractice.scala
├── build.sbt
└── output.txt
```

## Input Data

The project uses 10 customer transactions:

```text
customer_id,customer_name,transaction_amount
C001,Ravi,2500
C002,Priya,15000
C003,Arjun,75000
C004,Teja,4200
C005,Sneha,120000
C006,Kiran,8500
C007,Manoj,45000
C008,Anu,1800
C009,Varun,95000
C010,Divya,65000
```

## 1. Scala UDF — Risk Classification

A Scala UDF classifies transactions using these bands:

| Transaction Amount | Risk Category |
|---:|---|
| Less than 5,000 | LOW |
| 5,000 to less than 20,000 | MEDIUM |
| 20,000 to less than 80,000 | HIGH |
| 80,000 and above | CRITICAL |

The UDF is applied with `withColumn` to create the `risk_category` column.

## 2. withColumn

The program uses `withColumn` to add calculated information to the DataFrame.

For example:

```scala
val riskDF = transactionsDF.withColumn(
  "risk_category",
  classifyRisk(col("transaction_amount"))
)
```

## 3. Built-in Spark Function

A built-in Spark expression calculates the transaction amount including 18% tax:

```scala
round(col("transaction_amount") * 1.18, 2)
```

Built-in Spark functions are generally preferred when they can express the required logic because Spark can optimize them effectively.

## 4. UDF vs Built-in Function

### UDF

Used for custom business logic that is not directly available as a convenient built-in expression.

Example:

```text
Transaction Amount → Custom Risk Logic → Risk Category
```

### Built-in Function

Used for standard Spark-supported operations such as arithmetic and rounding.

Example:

```text
Transaction Amount → × 1.18 → Rounded Amount
```

## 5. Register UDF with Spark SQL

The program registers the UDF with the Spark session:

```scala
spark.udf.register(
  "classify_risk",
  (amount: Double) => {
    if (amount < 5000) "LOW"
    else if (amount < 20000) "MEDIUM"
    else if (amount < 80000) "HIGH"
    else "CRITICAL"
  }
)
```

A temporary view is then created and the registered UDF is called from Spark SQL.

## 6. Risk Classification Result

The successful execution produced:

```text
C001 Ravi   2500   LOW
C002 Priya  15000  MEDIUM
C003 Arjun  75000  HIGH
C004 Teja   4200   LOW
C005 Sneha  120000 CRITICAL
C006 Kiran  8500   MEDIUM
C007 Manoj  45000  HIGH
C008 Anu    1800   LOW
C009 Varun  95000  CRITICAL
C010 Divya  65000  HIGH
```

## 7. Risk Category Summary

```text
CRITICAL : 2
HIGH     : 3
LOW      : 3
MEDIUM   : 2
```

## 8. Final Statistics

```text
Total Customers : 10
Total Transaction Amount : 432000
```

## How to Run

Compile:

```bash
sbt compile
```

Run:

```bash
sbt run
```

Save output:

```bash
sbt run > output.txt 2>&1
```

View the final output:

```bash
tail -15 output.txt
```

## Screenshots

The project contains four execution screenshots:

- `day15-output-1.png`
- `day15-output-2.png`
- `day15-output-3.png`
- `day15-output-4.png`

## Result

The Day 15 UDF practice was successfully completed.

```text
DAY 15 COMPLETED SUCCESSFULLY
```
