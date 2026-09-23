# Day 14 — DataFrame and Dataset

## Objective

- Create a case class and convert DataFrame to Dataset.
- Convert Dataset back to DataFrame.
- Compare RDD, DataFrame and Dataset.
- Explain type safety and Catalyst optimization.
- Build a typed employee payroll pipeline.

## Technologies

- Apache Spark 3.5.3
- Scala 2.12.18
- SBT 2.0.7
- Java 17.0.20
- Ubuntu WSL2

## Project Structure

```text
Day-14-DataFrame-Dataset/
├── data/
│   └── employees.csv
├── screenshots/
│   ├── day14-output-1.png
│   ├── day14-output-2.png
│   ├── day14-output-3.png
│   ├── day14-output-4.png
│   └── day14-output-5.png
├── project/
│   └── build.properties
├── src/
│   └── main/
│       └── scala/
│           └── Day14DataFrameDataset.scala
├── build.sbt
└── output.txt
```

## Employee Dataset

The `employees.csv` file contains 10 employees with:

- Employee ID
- Name
- Department
- Age
- Monthly Salary

Example:

```text
employee_id,name,department,age,salary
E001,Ravi,IT,24,55000
E002,Priya,HR,26,62000
E003,Arjun,Finance,29,75000
...
E010,Divya,Finance,26,90000
```

## 1. Create DataFrame

The CSV file is read as a Spark DataFrame using:

```scala
val employeeDF = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/employees.csv")
```

The DataFrame contains 10 employee records.

## 2. Create Case Class

A typed `Employee` case class is used:

```scala
case class Employee(
  employee_id: String,
  name: String,
  department: String,
  age: Int,
  salary: Double
)
```

## 3. DataFrame to Dataset

Using Spark's encoder support:

```scala
import spark.implicits._

val employeeDS = employeeDF.as[Employee]
```

The DataFrame is converted into a typed `Dataset[Employee]`.

## 4. Typed Dataset Operation

The Dataset is filtered using a typed Scala function:

```scala
val highSalaryEmployees =
  employeeDS.filter(employee => employee.salary >= 65000)
```

The program identifies employees whose monthly salary is at least ₹65,000.

## 5. Dataset Back to DataFrame

The Dataset is converted back to a DataFrame:

```scala
val employeeDFAgain = employeeDS.toDF()
```

## 6. Typed Employee Payroll Pipeline

A typed `Payroll` case class is used to build the payroll pipeline.

The program calculates:

- Annual salary = monthly salary × 12
- Bonus = 10% for salary ≥ ₹80,000
- Bonus = 5% for salary below ₹80,000
- Total pay = annual salary + bonus

Example:

```scala
val payrollDS = employeeDS.map { employee =>
  val annualSalary = employee.salary * 12
  val bonus =
    if (employee.salary >= 80000) employee.salary * 0.10
    else employee.salary * 0.05

  val totalPay = annualSalary + bonus

  Payroll(
    employee.employee_id,
    employee.name,
    employee.department,
    employee.salary,
    annualSalary,
    bonus,
    totalPay
  )
}
```

## 7. Department-Wise Payroll

The payroll DataFrame is grouped by department to calculate:

- Employee count
- Total annual salary
- Total bonus
- Total payroll

Results from the execution:

```text
Finance   -> 3 employees -> Total Payroll 2984950.0
IT        -> 3 employees -> Total Payroll 2349750.0
HR        -> 2 employees -> Total Payroll 1518300.0
Marketing -> 2 employees -> Total Payroll 1421900.0
```

## 8. RDD vs DataFrame vs Dataset

### RDD

RDD is Spark's lower-level distributed collection API. It provides flexibility and object-oriented transformations but does not use Spark SQL's Catalyst optimizer for arbitrary RDD transformations.

### DataFrame

A DataFrame is a distributed table of `Row` objects with a schema. It supports DataFrame and SQL APIs and benefits from Spark SQL optimization.

### Dataset

A Dataset is a distributed collection with a defined type. In Scala, typed Dataset operations provide compile-time type checking while still benefiting from Spark SQL optimization.

### Comparison

| Feature | RDD | DataFrame | Dataset |
|---|---|---|---|
| Abstraction | Low level | Structured | Structured + typed |
| Schema | No fixed schema | Yes | Yes |
| Type safety | Runtime/object level | Row-based | Compile-time for typed operations |
| Catalyst optimization | No | Yes | Yes |
| SQL/DataFrame API | No | Yes | Yes |

## 9. Type Safety

Dataset provides compile-time type safety for typed operations.

For example:

```scala
employeeDS.filter(employee => employee.salary >= 65000)
```

The compiler knows that `employee` is an `Employee` object and that `salary` is a field of that case class.

## 10. Catalyst Optimization

Catalyst is Spark SQL's query optimizer. DataFrame and Dataset operations can be represented as structured query plans that Spark can optimize before execution.

RDD transformations are lower-level operations and do not go through Catalyst query-plan optimization.

## 11. Final Payroll Statistics

The successful execution produced:

```text
Total Employees : 10
Total Monthly Salary : 686000
Average Monthly Salary : 68600.0
```

## How to Run

Compile the project:

```bash
sbt compile
```

Run the application:

```bash
sbt run
```

Save the execution output:

```bash
sbt run > output.txt 2>&1
```

## Result

The Day 14 program successfully demonstrates:

- Case class creation
- DataFrame creation from CSV
- DataFrame to Dataset conversion
- Typed Dataset filtering
- Dataset to DataFrame conversion
- Typed employee payroll processing
- Department-wise payroll analysis
- RDD vs DataFrame vs Dataset comparison
- Type safety
- Catalyst optimization
- Final payroll statistics

```text
DAY 14 COMPLETED SUCCESSFULLY
```

## Screenshots

- day14-output-1.png
- day14-output-2.png
- day14-output-3.png
- day14-output-4.png
- day14-output-5.png

## Status

Day 14 - DataFrame and Dataset: Completed Successfully
