# Day 15 — UDF Practice

## Objective

- Create a Scala UDF to classify customer transaction risk.
- Add calculated columns using `withColumn`.
- Compare a UDF with a built-in Spark function.
- Register a UDF with the Spark session/catalog.
- Create a customer risk category from transaction values.

## Technologies

- Scala 2.12.18
- Apache Spark 3.5.3
- Spark SQL
- sbt 2.0.7
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

The project uses 10 customer transactions.

| Customer | Name | Transaction Amount |
|---|---|---:|
| C001 | Ravi | 2500 |
| C002 | Priya | 15000 |
| C003 | Arjun | 75000 |
| C004 | Teja | 4200 |
| C005 | Sneha | 120000 |
| C006 | Kiran | 8500 |
| C007 | Manoj | 45000 |
| C008 | Anu | 1800 |
| C009 | Varun | 95000 |
| C010 | Divya | 65000 |

## 1. Scala UDF — Risk Classification

The Scala UDF classifies transaction amounts into four risk categories:

| Transaction Amount | Risk Category |
|---:|---|
| Less than 5,000 | LOW |
| 5,000 to less than 20,000 | MEDIUM |
| 20,000 to less than 80,000 | HIGH |
| 80,000 and above | CRITICAL |

The UDF is applied with `withColumn` to create the `risk_category` column.

## 2. withColumn

The program adds calculated columns to the DataFrame.

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

This demonstrates the difference between custom UDF logic and a built-in Spark expression.

## 4. UDF vs Built-in Function

### UDF

Used for custom business logic.

```text
Transaction Amount → Custom Risk Logic → Risk Category
```

### Built-in Function

Used for standard Spark-supported operations.

```text
Transaction Amount → × 1.18 → Rounded Amount
```

## 5. Registering the UDF

The UDF is registered with the Spark session/catalog:

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

A temporary view is created and the registered UDF is also used through Spark SQL.

## 6. Risk Category Summary

The successful execution produced:

```text
CRITICAL : 2
HIGH     : 3
LOW      : 3
MEDIUM   : 2
```

## 7. Final Statistics

```text
Total Customers : 10
Total Transaction Amount : 432000

DAY 15 COMPLETED SUCCESSFULLY
```

## How to Run

From the Day 15 directory:

```bash
sbt compile
sbt run
```

To save the output:

```bash
sbt run > output.txt 2>&1
```

To view the final output:

```bash
tail -15 output.txt
```

## Screenshots

The execution screenshots are stored in the `screenshots/` directory:

- `day15-output-1.png`
- `day15-output-2.png`
- `day15-output-3.png`
- `day15-output-4.png`

## Completion

**Day 15 UDF Practice completed successfully.**
