# Spark Functional Pipeline (Scala + Apache Spark)

> A complete, testable data analytics pipeline built with **Scala 2.12.19** and **Apache Spark 3.5.1**, showcasing advanced **functional programming** (FP) alongside practical Spark operations.

- **Team Manager:** Yuval Betito
- **Email:** yuval36811@gmail.com
- **Course:** Functional Programming (Scala + Apache Spark)
- **Submission Date:** 21.09.2025
- **Demo Video:** https://youtu.be/B2LUATUUkIk
- **GitHub:** https://github.com/Yuval-Betito/spark-functional-pipeline2.git

---

## Table of Contents
1. [Project Overview](#project-overview)
2. [Key Features](#key-features)
3. [Architecture](#architecture)
4. [Data Schema](#data-schema)
5. [Quick Start](#quick-start)
6. [CLI Usage](#cli-usage)
7. [Verifying Results](#verifying-results)
8. [Project Structure](#project-structure)
9. [Spark Operations & FP Techniques](#spark-operations--fp-techniques)
10. [Testing](#testing)
11. [Troubleshooting (Windows)](#troubleshooting-windows)
12. [Tech Stack](#tech-stack)
13. [License](#license)

---

## Project Overview
The pipeline ingests two CSV datasets—**transactions** and **products**—and computes **revenue per product category**.  
All domain logic is kept **pure and composable** under `core/*`, while I/O & Spark orchestration live under `app/*`.  
Results are produced in two ways:
- A **Spark-based** aggregation.
- A **pure FP** recomputation used for cross-checking.

A verifier tool (`tools/Verify.scala`) compares both outputs and confirms numeric equivalence.

---

## Key Features
- **Deterministic data generator** (`DataGen.scala`) → `data/products.csv`, `data/transactions.csv` (default **20,000** rows).
- **Typed Spark jobs** (`SparkJobs.scala`) → load, enrich, join, aggregate, and write results.
- **Pure FP core** (`core/*`) → ADT errors, tail recursion, currying/closures, custom combinators.
- **Parity check** (`tools/Verify.scala`) → asserts Spark vs pure equivalence.
- **Full test suite** → unit tests for FP core + end-to-end Spark test.

---

## Architecture
- **app/** – Spark entry points & orchestration (`Main`, `SparkJobs`, `DataGen`).
- **core/** – Pure domain: models, parsers, validation, combinators, and business logic.
- **tools/** – Verifier to diff Spark vs pure outputs.
- **test/** – Core unit tests and an end-to-end pipeline test.

This separation ensures the core logic is **side-effect free** and **highly testable**, while Spark remains a thin I/O layer.

---

## Data Schema
**products.csv**
```
productId,category
p1,Books
...
```

**transactions.csv**
```
txnId,userId,productId,quantity,unitPrice,timestamp
t1,u1,p1,2,10.50,1711300000000
...
```

---

## Quick Start

### Requirements
- **JDK 11**
- **sbt**
- **Scala 2.12.19**
- **Spark 3.5.1** (managed as a dependency)
- Windows users: see [Troubleshooting (Windows)](#troubleshooting-windows)

### 1) Clone
```bash
git clone https://github.com/Yuval-Betito/spark-functional-pipeline2.git
cd spark-functional-pipeline2
```

### 2) Build
```bash
sbt clean compile
```

### 3) Generate Data (20k rows by default)
```bash
sbt "runMain com.example.pipeline.app.DataGen"
# outputs: data/products.csv, data/transactions.csv
```

### 4) Run the Pipeline
```bash
sbt "runMain com.example.pipeline.app.Main data/transactions.csv data/products.csv out 0.0"
# outputs: out/revenue_by_category, out/revenue_pure
```

---

## CLI Usage
`Main` accepts:
```
args(0): transactions.csv path     (default: data/transactions.csv)
args(1): products.csv path         (default: data/products.csv)
args(2): output directory          (default: out)
args(3): min total threshold       (default: 0.0)
```

Example with threshold:
```bash
sbt "runMain com.example.pipeline.app.Main data/transactions.csv data/products.csv out 100.0"
```

`DataGen` accepts an optional row count:
```bash
# 50k synthetic transactions
sbt "runMain com.example.pipeline.app.DataGen 50000"
```

---

## Verifying Results
Compare Spark aggregation with the pure FP recomputation:
```bash
sbt "runMain com.example.pipeline.tools.Verify out"
# prints: VERIFY OK  (or a detailed diff on mismatch)
```

---

## Project Structure
```
.
├── build.sbt
├── src
│   ├── main/scala/com/example/pipeline
│   │   ├── app
│   │   │   ├── DataGen.scala
│   │   │   ├── Main.scala
│   │   │   └── SparkJobs.scala
│   │   ├── core
│   │   │   ├── Combinators.scala
│   │   │   ├── Errors.scala
│   │   │   ├── Logic.scala
│   │   │   └── Model.scala
│   │   └── tools/Verify.scala
│   └── test/scala/com/example/pipeline
│       ├── app/EndToEndSpec.scala
│       └── core/{CombinatorsSpec,LogicSpec,UnifiedParsingSpec}.scala
├── data/                 # created by DataGen
└── out/                  # created by Main
```

---

## Spark Operations & FP Techniques

### Spark (in `app/SparkJobs.scala`)
- CSV ingest with headers: `spark.read.option("header","true").csv(...)`
- Typed casting & selection.
- Join on `productId` to enrich transactions.
- Derived column: `withColumn("total", col("quantity") * col("unitPrice"))`
- Filter with **curried closure predicate** from FP core: `Logic.minTotalFilter(threshold) _`
- Aggregation: `groupBy("category").agg(count, sum("total")).orderBy(desc("revenue"))`
- Write results to CSV (coalesce to 1 file for readability).

### Advanced FP (in `core/*`)
- **Custom combinators** (`Combinators.scala`): `pipe`, `composeAll`, `where`, `mapWhere`
- **Currying & closures**: `minTotalFilter(threshold)(t)`
- **Tail recursion**: `sumTailRec`, `meanTailRec`
- **Functional error handling with ADTs**: `Either[ParseError, A]` from pure CSV parsers in `Model.scala` and error hierarchy in `Errors.scala`
- **Pure business logic**: `Logic.revenueByCategory` over in-memory pairs

### Equivalence Check
- `tools/Verify.scala` reads the Spark CSV and the pure CSV, maps `category → value`, and asserts numeric equality within an epsilon (`1e-9`).

---

## Testing
Run all tests:
```bash
sbt test
```
Includes:
- **Core unit tests**: parsing to `Either`, tail-rec helpers, combinators, aggregation logic, curried predicates.
- **End-to-end** (`EndToEndSpec`): generates 10k rows, runs the full pipeline, validates revenue and outputs.

> In `build.sbt`, tests are forked and not parallelized for Spark stability.

---

## Troubleshooting (Windows)
- Dependencies pin **Hadoop client** versions and set:
    - `SPARK_LOCAL_IP=127.0.0.1` for stable local binding.
    - JDK `--add-opens` flags to silence reflective warnings.
- `Main.printEnvironmentInfo()` prints environment details (Spark/Hadoop versions, `HADOOP_HOME`, etc.).
- If you see local Hadoop binary issues, ensure `HADOOP_HOME` points to a valid distribution (with `bin/`).

---

## Tech Stack
- **Scala:** 2.12.19
- **Apache Spark:** 3.5.1
- **Hadoop Client:** 3.3.5
- **Testing:** ScalaTest 3.2.19
- **Build Tool:** sbt

---

## License
This project is for academic purposes as part of the Functional Programming course. If you intend to reuse it, please credit the author and verify license compatibility of third-party components.

---

## Author
**Yuval Betito** — yuval36811@gmail.com
