# Troubleshooting

## Issue 1: Scala Compile Server Error

### Problem

Running the Scala program normally caused a Java tools-related compile server error.

### Solution

The program was executed with the `-nc` option:

```bash
scala -nc code/Day01.scala
