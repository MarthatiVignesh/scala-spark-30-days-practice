# Day 1 - Scala Essentials

## Objective

Practice Scala fundamentals using immutable collections.

## Topics Covered

- val
- var
- lazy val
- List
- Vector
- Set
- Map
- for-comprehension
- yield
- Traits
- Classes
- Immutable collections

## Project: Student Grade Processor

The program:

1. Stores student names and marks.
2. Calculates grades.
3. Finds students scoring 80 or above.
4. Calculates average marks.
5. Demonstrates List, Vector, Set and Map.
6. Demonstrates val, var and lazy val.
7. Uses a for-comprehension with yield.
8. Demonstrates a Logger trait with two implementations.

## Sample Students

| Student | Marks | Grade |
|---------|------:|-------|
| Teju    | 85    | B     |
| Ravi    | 72    | C     |
| Anu     | 91    | A     |
| Kiran   | 64    | D     |

## Output

```text
===== Scala Essentials - Day 1 =====

--- Students and Grades ---
Teju -> Marks: 85 -> Grade: B
Ravi -> Marks: 72 -> Grade: C
Anu -> Marks: 91 -> Grade: A
Kiran -> Marks: 64 -> Grade: D

--- Students who scored 80 or above ---
Teju -> 85
Anu -> 91

--- Average Marks ---
Average marks: 78.0

--- Logger Demo ---
[STUDENT] Student grade processed
[ADMIN] Grade report generated
