# Day 13 - Spark SQL Basics

## Objective

Learn Spark SQL basics using DataFrames, DataFrame transformations, temporary views, and SQL queries.

## Topics Covered

- Create a DataFrame from CSV data
- Inspect DataFrame schema
- Select specific columns
- Filter DataFrame records
- Use `withColumn` and expressions
- Create a temporary view
- Execute SQL queries using Spark SQL
- Perform city-wise customer analytics
- Generate a customer analytics report

## Technologies

- Apache Spark 3.5.3
- Scala 2.12.18
- SBT 2.0.7
- Java 17
- Ubuntu WSL2

## Project Structure

```text
Day-13-Spark-SQL-Basics/
├── data/
│   └── customers.csv
├── screenshots/
│   ├── day13-output-1.png
│   ├── day13-output-2.png
│   ├── day13-output-3.png
│   ├── day13-output-4.png
│   └── day13-output-5.png
├── project/
│   └── build.properties
├── src/
│   └── main/
│       └── scala/
│           └── Day13SparkSQLBasics.scala
├── build.sbt
├── output.txt
└── README.md
```

## Dataset

The `customers.csv` file contains:

- Customer ID
- Customer Name
- City
- Age
- Purchase Amount
- Customer Status

The dataset contains 10 customers.

## 1. Create DataFrame from CSV

The program reads the CSV file using Spark DataFrame APIs:

```scala
val customersDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/customers.csv")
```

The DataFrame contains 10 customer records.

## 2. Inspect Schema

The program uses:

```scala
customersDF.printSchema()
```

The schema contains:

```text
customer_id: string
name: string
city: string
age: integer
purchase_amount: integer
status: string
```

## 3. Select Columns

Selected customer information using:

```scala
customersDF.select(
  "customer_id",
  "name",
  "city",
  "purchase_amount"
)
```

## 4. Filter Customers

Active customers are filtered using:

```scala
customersDF.filter(col("status") === "Active")
```

There are 8 active customers and 2 inactive customers.

High-value customers are filtered using a purchase amount of ₹60,000 or more.

## 5. withColumn and Expressions

A customer category is created using `withColumn` and `when`:

```scala
when(col("purchase_amount") >= 80000, "Premium")
  .when(col("purchase_amount") >= 50000, "Regular")
  .otherwise("Basic")
```

Categories:

- Premium: purchase amount >= ₹80,000
- Regular: purchase amount >= ₹50,000
- Basic: below ₹50,000

The program also calculates purchase amount including 18% tax.

## 6. Temporary View

The DataFrame is registered as a temporary SQL view:

```scala
customersDF.createOrReplaceTempView("customers")
```

The program confirms that the temporary view `customers` was created successfully.

## 7. Spark SQL Queries

The project uses Spark SQL to:

- Select active customers
- Sort customers by purchase amount
- Perform city-wise analytics
- Find the top 5 customers

Example:

```sql
SELECT customer_id, name, city, purchase_amount
FROM customers
WHERE status = 'Active'
ORDER BY purchase_amount DESC
```

## 8. City-Wise Customer Analytics

The SQL report calculates:

- Customer count
- Average purchase
- Total purchase

Results:

```text
Hyderabad  -> 3 customers -> Average ₹62666.67 -> Total ₹188000
Mumbai     -> 2 customers -> Average ₹87500.00 -> Total ₹175000
Bengaluru  -> 2 customers -> Average ₹58500.00 -> Total ₹117000
Chennai    -> 2 customers -> Average ₹45000.00 -> Total ₹90000
Delhi      -> 1 customer  -> Average ₹41000.00 -> Total ₹41000
```

## 9. Top 5 Customers

The top 5 customers by purchase amount are:

```text
C005 - Sneha - ₹92000
C010 - Divya - ₹83000
C004 - Teja  - ₹75000
C008 - Anu   - ₹68000
C002 - Priya - ₹62000
```

## 10. Customer Analytics Report

Final report:

```text
Total Customers     : 10
Active Customers    : 8
Inactive Customers  : 2
Total Purchase      : ₹611000
Average Purchase    : ₹61100.0
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

Save execution output:

```bash
sbt run > output.txt 2>&1
```

## Result

The Spark SQL program successfully demonstrates:

- DataFrame creation
- Schema inspection
- Column selection
- Filtering
- `withColumn`
- Spark SQL expressions
- Temporary views
- SQL queries
- Group-by analytics
- Customer analytics reporting

```text
DAY 13 COMPLETED SUCCESSFULLY
```

## Screenshots

- day13-output-1.png
- day13-output-2.png
- day13-output-3.png
- day13-output-4.png
- day13-output-5.png

## Status

Day 13 - Spark SQL Basics: Completed Successfully
