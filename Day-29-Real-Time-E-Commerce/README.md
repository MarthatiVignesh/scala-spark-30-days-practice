# Day 29 — Real-Time E-Commerce Analytics

## Objective

Implement an end-to-end Spark analytics pipeline covering:

- Raw → Clean → Enrich → Aggregate processing
- UDF-based product category classification
- Repartitioning by `customer_id`
- Pair RDD aggregation
- Window-based ranking
- Physical plan inspection

## Technologies

- Scala 2.12.18
- Apache Spark 3.5.3
- Spark SQL
- Spark Core
- Spark Streaming dependency
- sbt 2.0.7
- Java 17

## Input Data

### Orders

Schema:

`order_id, customer_id, product_id, timestamp, quantity, amount`

8 order records were processed.

### Products

Schema:

`product_id, product_name, category`

5 product records were used.

### Customers

Schema:

`customer_id, customer_name, customer_type`

4 customer records were used.

## Processing Pipeline

### 1. Raw Data

The application reads orders, products, and customers using explicit Spark schemas.

### 2. Cleaning

Orders are filtered for:

- Non-null identifiers
- Positive quantity
- Positive amount
- Valid timestamps

Result:

- Valid orders: **8**

### 3. Enrichment

Orders are joined with product and customer reference data.

A UDF classifies product categories:

- Electronics → Technology
- Accessories → Lifestyle
- Other categories → Other

### 4. Repartitioning

The enriched dataset is repartitioned by `customer_id` into **4 partitions**.

This prepares the data for customer-level aggregation.

### 5. Customer Revenue Aggregation

Customer totals:

| Customer | Revenue | Quantity | Orders |
|---|---:|---:|---:|
| C001 — Vignesh | ₹2600 | 9 | 3 |
| C004 — Meera | ₹1600 | 2 | 1 |
| C002 — Ravi | ₹1500 | 2 | 2 |
| C003 — Asha | ₹800 | 3 | 2 |

### 6. Pair RDD Aggregation

The same customer revenue totals were calculated using a Pair RDD:

`(customerId, amount).reduceByKey(...)`

Result: **Pair RDD aggregation completed successfully.**

### 7. Category Revenue

| Business Category | Revenue |
|---|---:|
| Technology | ₹3700 |
| Lifestyle | ₹2800 |

Total revenue:

**₹6500**

### 8. Window Ranking

A Spark SQL window was used with:

- Partition by business category
- Order by revenue descending
- `row_number()` for ranking

This provides category-wise product/customer revenue ranking.

### 9. Physical Plan

The Spark physical plan was inspected using `explain(true)`.

The plan demonstrates:

- Repartitioning by `customer_id` into 4 partitions
- Broadcast hash joins for reference data
- Hash aggregation
- Exchange/shuffle stages

## Final Result

- Valid enriched orders: **8**
- Total revenue: **₹6500.00**
- Customer partitions: **4**
- Pair RDD aggregation: **COMPLETED**
- UDF classification: **COMPLETED**
- Window ranking: **COMPLETED**
- Physical plan: **DISPLAYED**

## Project Structure

```text
Day-29-Real-Time-E-Commerce/
├── build.sbt
├── data/
│   ├── orders.txt
│   ├── products.txt
│   └── customers.txt
├── project/
│   └── build.properties
├── screenshots/
│   └── day29-ecommerce-analytics.png
└── src/
    └── main/
        └── scala/
            └── Day29RealTimeECommerce.scala
```

## Run

From the Day 29 directory:

```bash
sbt compile
sbt run
```

## Key Concepts Practiced

- Spark SQL
- Explicit schemas
- Data cleaning
- Data enrichment
- UDFs
- Broadcast joins
- Repartitioning
- Pair RDDs
- `reduceByKey`
- Window functions
- Physical plan analysis
- Performance-oriented Spark processing

## Status

**DAY 29 COMPLETED SUCCESSFULLY**