# The PySpark Handbook: From Zero to Cluster Optimization

> A complete, progressive guide for data engineers — from your first SparkSession to production-grade lakehouse pipelines.

---

## Table of Contents

1. [Introduction: Thinking in Distributed Systems](#introduction)
2. [Environment Setup](#environment-setup)
3. [The SparkSession and Basic I/O](#chapter-1)
4. [Data Manipulation (Transformations)](#chapter-2)
5. [Schema Management and Data Types](#chapter-3)
6. [Advanced Analytics](#chapter-4)
7. [Joins & The Art of Avoiding Shuffles](#chapter-5)
8. [User-Defined Functions (UDFs)](#chapter-6)
9. [Streaming with Structured Streaming](#chapter-7)
10. [Cluster Performance & Tuning](#chapter-8)
11. [The Modern Lakehouse (Delta Lake)](#chapter-9)
12. [Testing PySpark Code](#chapter-10)
13. [Best Practices Cheat Sheet](#chapter-11)

---

## Introduction: Thinking in Distributed Systems {#introduction}

Before writing a single line of PySpark, you have to shift how you think about data. In standard Python (like Pandas), your data sits on one machine's RAM. If the data gets too big, the machine crashes.

**Apache Spark** is a distributed computing engine. It takes your massive dataset and slices it into smaller chunks called **partitions**, distributing them across a cluster of computers (workers). When you write PySpark code, you are writing a set of instructions that a **Driver** node sends to all the **Worker** nodes to execute in parallel.

### The Spark Architecture

```
                    ┌─────────────────────────────────────┐
                    │           DRIVER NODE               │
                    │  - Your PySpark script runs here    │
                    │  - Builds the DAG (execution plan)  │
                    │  - Coordinates all workers          │
                    └───────────────┬─────────────────────┘
                                    │  sends tasks
              ┌─────────────────────┼──────────────────────┐
              ▼                     ▼                       ▼
     ┌──────────────┐     ┌──────────────┐       ┌──────────────┐
     │  WORKER NODE │     │  WORKER NODE │       │  WORKER NODE │
     │  Executor 1  │     │  Executor 2  │       │  Executor 3  │
     │  Partition A │     │  Partition B │       │  Partition C │
     └──────────────┘     └──────────────┘       └──────────────┘
```

### Two Golden Rules of Spark

1. **DataFrames are Immutable** — Once created, you cannot change a DataFrame. Every transformation creates a new DataFrame.

2. **Lazy Evaluation** — Spark doesn't process data immediately. When you tell it to filter or join, it just takes notes (creating a **Directed Acyclic Graph**, or DAG). It only actually executes the work when you call an **Action** (like `.show()`, `.count()`, or `.write()`).

### Transformations vs Actions

| Type | What it does | Examples | Triggers execution? |
|------|-------------|----------|-------------------|
| **Transformation** | Creates a new DataFrame | `filter`, `select`, `join`, `groupBy` | ❌ No |
| **Action** | Returns a result or writes data | `show`, `count`, `collect`, `write` | ✅ Yes |

> **Tip for beginners:** Think of transformations as building a recipe and actions as actually cooking it.

---

## Environment Setup {#environment-setup}

### Local Installation

```bash
# Step 1: Install Java (Spark runs on the JVM)
# On Ubuntu/Debian
sudo apt install default-jdk -y
java -version

# Step 2: Install PySpark via pip
pip install pyspark

# Step 3: Verify installation
python -c "import pyspark; print(pyspark.__version__)"
```

### Using PySpark in a Jupyter Notebook

```bash
pip install pyspark jupyter findspark

# In your notebook's first cell:
import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Notebook").getOrCreate()
```

### Using PySpark on Cloud Platforms

Most cloud platforms (Databricks, AWS EMR, Google Dataproc) come with Spark pre-installed. You typically just need to create a cluster and start writing code. No local install needed.

### Project Structure (Recommended)

```
my_spark_project/
├── jobs/
│   ├── etl_job.py          # Main entry point
│   └── transformations.py  # Reusable transform logic
├── tests/
│   └── test_transformations.py
├── configs/
│   └── spark_config.py     # SparkSession configs
└── requirements.txt
```

---

## Chapter 1: The SparkSession and Basic I/O {#chapter-1}

The `SparkSession` is your steering wheel. It is the single entry point for reading data, executing SQL, and managing cluster configurations.

### 1.1 Initializing the Session

```python
from pyspark.sql import SparkSession

# The builder pattern is used to configure and create the session
spark = SparkSession.builder \
    .appName("Daily_ETL_Job") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Always stop the session when done (important in local/test environments)
# spark.stop()
```

> **Theory Note:** The `spark.sql.shuffle.partitions` config is crucial. When Spark moves data around (a "shuffle"), it defaults to splitting the data into 200 partitions. For small datasets, this creates too much overhead. For huge datasets, you may need more. A good rule of thumb: aim for partitions of ~128MB each.

### 1.2 Reading Data

Data engineering relies heavily on picking the right file format. While CSV and JSON are human-readable, they are terrible for big data because they are row-based and uncompressed. **Parquet** is a columnar format, highly compressed, and the industry standard for Spark.

```python
# Reading a CSV (Spark has to infer the schema, which is slow and error-prone)
df_csv = spark.read.csv("path/sales.csv", header=True, inferSchema=True)

# Better: Define the schema explicitly (faster, safer in production)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

schema = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("purchase_amount", DoubleType(), nullable=True),
    StructField("region", StringType(), nullable=True)
])
df_csv_typed = spark.read.csv("path/sales.csv", header=True, schema=schema)

# Reading Parquet (Schema is built-in, highly optimized)
df_parquet = spark.read.parquet("path/sales_data/")

# Reading JSON
df_json = spark.read.json("path/events.json")

# Reading from a database (JDBC)
df_db = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://host:3306/mydb") \
    .option("dbtable", "sales") \
    .option("user", "user") \
    .option("password", "password") \
    .load()
```

### 1.3 Writing Data

```python
# Write to Parquet (recommended for intermediate storage)
df_sales.write.mode("overwrite").parquet("path/output/")

# Write to CSV (for downstream tools that need it)
df_sales.write.mode("overwrite").option("header", True).csv("path/output_csv/")

# Write to a database
df_sales.write.format("jdbc") \
    .option("url", "jdbc:mysql://host:3306/mydb") \
    .option("dbtable", "sales_output") \
    .option("user", "user") \
    .option("password", "password") \
    .mode("append") \
    .save()
```

### Write Modes

| Mode | Behaviour |
|------|-----------|
| `overwrite` | Delete existing data, write new data |
| `append` | Add to existing data |
| `ignore` | Do nothing if data already exists |
| `error` (default) | Throw an error if data exists |

### 1.4 Exploring Your DataFrame

```python
# See the first 5 rows
df_parquet.show(5)

# See the schema (column names and types)
df_parquet.printSchema()

# Count rows
print(df_parquet.count())

# See summary statistics
df_parquet.describe("purchase_amount", "customer_id").show()

# See how many partitions your data is split into
print(df_parquet.rdd.getNumPartitions())
```

---

## Chapter 2: Data Manipulation (Transformations) {#chapter-2}

Transformations are how we clean and shape our data.

### 2.1 Selecting, Filtering, and Creating Columns

Use the `col` function to reference columns programmatically. `lit` is used to inject literal (constant) values.

```python
from pyspark.sql.functions import col, lit, when

# 1. Select specific columns
df_subset = df_parquet.select("customer_id", "purchase_amount")

# 2. Filter data (WHERE clause)
df_high_value = df_parquet.filter(col("purchase_amount") > 1000)

# Combining multiple conditions
df_filtered = df_parquet.filter(
    (col("purchase_amount") > 100) & 
    (col("region") == "WEST") &
    col("customer_id").isNotNull()
)

# 3. Create or modify a column (withColumn)
# If the column exists, it overwrites it. If not, it creates a new one.
df_taxed = df_high_value \
    .withColumn("tax_amount", col("purchase_amount") * 0.08) \
    .withColumn("status", lit("PROCESSED"))

# 4. Conditional column using when/otherwise (equivalent to CASE WHEN in SQL)
df_tier = df_parquet.withColumn(
    "customer_tier",
    when(col("purchase_amount") >= 10000, "PLATINUM")
    .when(col("purchase_amount") >= 1000, "GOLD")
    .when(col("purchase_amount") >= 100, "SILVER")
    .otherwise("BRONZE")
)

# 5. Rename a column
df_renamed = df_parquet.withColumnRenamed("purchase_amount", "amount_usd")

# 6. Drop a column
df_dropped = df_parquet.drop("unwanted_column")
```

### 2.2 Working with Strings

```python
from pyspark.sql.functions import (
    upper, lower, trim, length, substring, 
    concat, concat_ws, split, regexp_replace, like
)

df_strings = df_parquet \
    .withColumn("region_upper", upper(col("region"))) \
    .withColumn("email_clean", trim(lower(col("customer_email")))) \
    .withColumn("name_len", length(col("customer_name"))) \
    .withColumn("first_3", substring(col("customer_id"), 1, 3)) \
    .withColumn("full_label", concat_ws("-", col("region"), col("customer_id"))) \
    .withColumn("phone_clean", regexp_replace(col("phone"), "[^0-9]", ""))
```

### 2.3 Working with Dates and Timestamps

```python
from pyspark.sql.functions import (
    current_date, current_timestamp, to_date, to_timestamp,
    date_format, datediff, months_between, year, month, dayofweek,
    date_add, date_sub
)

df_dates = df_parquet \
    .withColumn("today", current_date()) \
    .withColumn("days_since_purchase", datediff(current_date(), col("purchase_date"))) \
    .withColumn("purchase_year", year(col("purchase_date"))) \
    .withColumn("purchase_month", month(col("purchase_date"))) \
    .withColumn("day_of_week", dayofweek(col("purchase_date")))  # 1=Sunday, 7=Saturday

# Parse a string into a date
df_parsed = df_parquet.withColumn(
    "purchase_date",
    to_date(col("purchase_date_str"), "yyyy-MM-dd")
)
```

### 2.4 Handling Messy Data (Nulls and Duplicates)

Real-world data is dirty. Spark provides `.na` and `.dropDuplicates` methods to handle this at scale.

```python
# Remove rows where ANY column is null
df_no_nulls = df_parquet.na.drop()

# Remove rows where specific columns are null
df_no_nulls_id = df_parquet.na.drop(subset=["customer_id", "purchase_amount"])

# Fill null values with defaults based on column type
df_cleaned = df_parquet.na.fill({
    "tax_amount": 0.0,
    "customer_email": "unknown@domain.com",
    "region": "UNKNOWN"
})

# Replace specific values (e.g., replace sentinel values with null)
from pyspark.sql.functions import when
df_replaced = df_parquet.withColumn(
    "purchase_amount",
    when(col("purchase_amount") == -1, None).otherwise(col("purchase_amount"))
)

# Remove identical rows
df_unique = df_parquet.dropDuplicates()

# Remove rows only if the specific ID is duplicated (keep first occurrence)
df_unique_ids = df_parquet.dropDuplicates(["customer_id"])
```

---

## Chapter 3: Schema Management and Data Types {#chapter-3}

Schema control is what separates a data scientist writing one-off notebooks from a data engineer building reliable pipelines.

### 3.1 Defining Schemas Explicitly

Always define schemas explicitly in production. `inferSchema=True` reads the data twice (once to infer types, once to load), is slow, and can silently guess wrong.

```python
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    DoubleType, FloatType, BooleanType,
    DateType, TimestampType,
    ArrayType, MapType
)

# Flat schema
sales_schema = StructType([
    StructField("sale_id",        LongType(),   nullable=False),
    StructField("customer_id",    StringType(), nullable=False),
    StructField("product_id",     StringType(), nullable=True),
    StructField("amount",         DoubleType(), nullable=True),
    StructField("sale_date",      DateType(),   nullable=True),
    StructField("is_returned",    BooleanType(),nullable=True),
])

# Nested schema (for JSON data)
event_schema = StructType([
    StructField("event_id", StringType(), nullable=False),
    StructField("user", StructType([
        StructField("user_id", StringType()),
        StructField("country", StringType())
    ])),
    StructField("tags", ArrayType(StringType())),           # Array column
    StructField("metadata", MapType(StringType(), StringType())) # Map column
])
```

### 3.2 Casting Types

```python
# Cast a column to a different type
df_cast = df_parquet \
    .withColumn("amount", col("amount").cast(DoubleType())) \
    .withColumn("sale_id", col("sale_id").cast(LongType()))

# Or using string shorthand
df_cast2 = df_parquet.withColumn("amount", col("amount").cast("double"))
```

### 3.3 Working with Nested Data (JSON/Struct columns)

```python
from pyspark.sql.functions import col, explode, from_json

# Access a nested field
df_flat = df_events.select(
    col("event_id"),
    col("user.user_id").alias("user_id"),
    col("user.country").alias("country")
)

# Explode an array column into multiple rows
df_exploded = df_events.select(
    col("event_id"),
    explode(col("tags")).alias("tag")
)

# Parse a JSON string column into a struct
df_parsed = df_raw.withColumn(
    "parsed_payload",
    from_json(col("json_string_col"), event_schema)
)
```

---

## Chapter 4: Advanced Analytics {#chapter-4}

### 4.1 Grouping and Aggregations

When you want to summarize data (e.g., total sales per region), you use `groupBy`. Spark will group all identical keys together, which requires moving data across the network (a **shuffle**).

```python
from pyspark.sql.functions import sum, avg, count, max, min, countDistinct, collect_list

df_summary = df_cleaned.groupBy("region").agg(
    sum("purchase_amount").alias("total_revenue"),
    avg("purchase_amount").alias("average_order_value"),
    count("customer_id").alias("total_transactions"),
    countDistinct("customer_id").alias("unique_customers"),
    max("purchase_amount").alias("largest_order"),
    collect_list("product_id").alias("all_products_list")
)

# Multi-column groupBy
df_monthly = df_cleaned.groupBy("region", "purchase_year", "purchase_month").agg(
    sum("purchase_amount").alias("monthly_revenue")
)
```

### 4.2 Window Functions

> **Theory:** What if you want to rank employees by salary within their department, but you don't want to collapse the data into a summary like `groupBy` does? You use **Window functions**. They perform calculations across a set of rows related to the current row, without reducing the number of rows.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lead, lag, sum, avg

# Define the "Window" (Group by department, sort by salary descending)
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

# row_number: 1 to the highest paid, 2 to the next — no ties
df_ranked = df_employees.withColumn("salary_rank", row_number().over(window_spec))

# rank: allows ties, but gaps in rank (1, 1, 3...)
df_rank = df_employees.withColumn("salary_rank", rank().over(window_spec))

# dense_rank: allows ties, no gaps (1, 1, 2...)
df_dense = df_employees.withColumn("salary_rank", dense_rank().over(window_spec))

# Filter to get only the top earner per department
df_top_earners = df_ranked.filter(col("salary_rank") == 1)

# lead(): peek at the NEXT row's value
df_next = df_employees.withColumn(
    "next_highest_salary",
    lead("salary", 1).over(window_spec)
)

# lag(): peek at the PREVIOUS row's value
df_prev = df_employees.withColumn(
    "previous_salary",
    lag("salary", 1).over(window_spec)
)

# Running total (cumulative sum)
window_running = Window.partitionBy("region") \
    .orderBy("sale_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_cumulative = df_sales.withColumn(
    "cumulative_revenue",
    sum("purchase_amount").over(window_running)
)

# Rolling 7-day average
window_rolling = Window.partitionBy("customer_id") \
    .orderBy("sale_date") \
    .rowsBetween(-6, 0)  # current row + 6 preceding rows

df_rolling = df_sales.withColumn(
    "rolling_7day_avg",
    avg("purchase_amount").over(window_rolling)
)
```

### 4.3 Pivoting and Unpivoting

```python
# PIVOT: Turn row values into column headers
# e.g., one column per region showing total sales
df_pivot = df_sales.groupBy("product_id").pivot("region", ["EAST", "WEST", "NORTH", "SOUTH"]) \
    .agg(sum("purchase_amount"))

# UNPIVOT / STACK: Turn columns back into rows
# (Spark doesn't have a native unpivot until Spark 3.4, so we use a workaround)
from pyspark.sql.functions import expr

df_unpivot = df_pivot.select(
    "product_id",
    expr("stack(4, 'EAST', EAST, 'WEST', WEST, 'NORTH', NORTH, 'SOUTH', SOUTH) as (region, revenue)")
)
```

---

## Chapter 5: Joins & The Art of Avoiding Shuffles {#chapter-5}

Joining tables is the most expensive operation in Spark because it forces the cluster to physically move massive amounts of data across the network to align matching keys.

### 5.1 Standard Joins

```python
# Inner join (Keep only matching rows)
df_joined = df_sales.join(df_customers, df_sales.cust_id == df_customers.id, "inner")

# Left join (Keep all rows from the left, nulls for non-matching right)
df_left = df_sales.join(df_customers, df_sales.cust_id == df_customers.id, "left")

# Right join
df_right = df_sales.join(df_customers, df_sales.cust_id == df_customers.id, "right")

# Full outer join
df_full = df_sales.join(df_customers, df_sales.cust_id == df_customers.id, "full")

# Left Anti Join (Find sales that have NO matching customer — great for finding data errors!)
df_orphans = df_sales.join(df_customers, df_sales.cust_id == df_customers.id, "left_anti")

# Left Semi Join (Keep only left rows that DO have a match, without bringing in right columns)
df_has_match = df_sales.join(df_customers, df_sales.cust_id == df_customers.id, "left_semi")
```

### 5.2 The Broadcast Join (Optimization Technique)

> **Theory:** If you have a massive table (Sales — 1 Billion rows) and a tiny table (Country Codes — 200 rows), a normal join will shuffle the 1 Billion rows. Instead, we can **Broadcast** the tiny table. Spark will copy the tiny table to the memory of every single worker node, allowing local joins without any network shuffle.

```python
from pyspark.sql.functions import broadcast

# Spark will ship df_country_codes to all nodes, bypassing the shuffle entirely
df_optimized = df_sales.join(broadcast(df_country_codes), "country_id")
```

> **Rule of thumb:** Broadcast tables smaller than ~10MB (configurable via `spark.sql.autoBroadcastJoinThreshold`).

### 5.3 Avoiding Column Ambiguity After Joins

When both DataFrames have a column with the same name, Spark gets confused.

```python
# Problem: both tables have a "status" column
df_joined = df_sales.join(df_customers, "customer_id")  # ambiguous "status"

# Solution 1: Rename before joining
df_customers_renamed = df_customers.withColumnRenamed("status", "customer_status")
df_joined = df_sales.join(df_customers_renamed, "customer_id")

# Solution 2: Select specific columns after joining
df_joined = df_sales.join(df_customers, "customer_id").select(
    df_sales["status"].alias("sale_status"),
    df_customers["status"].alias("customer_status"),
    "customer_id"
)
```

---

## Chapter 6: User-Defined Functions (UDFs) {#chapter-6}

Sometimes built-in Spark functions aren't enough. UDFs let you write arbitrary Python logic and apply it to a column — but they come with a significant performance cost.

### 6.1 Regular UDFs (Slow but Flexible)

When you use a Python UDF, Spark has to:
1. Serialize each row from the JVM to Python
2. Run your Python function
3. Serialize the result back to the JVM

This Python ↔ JVM serialization overhead can make UDFs **10x–100x slower** than native Spark functions.

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define a Python function
def categorize_amount(amount):
    if amount is None:
        return "UNKNOWN"
    if amount > 10000:
        return "HIGH"
    elif amount > 1000:
        return "MEDIUM"
    else:
        return "LOW"

# Register it as a UDF
categorize_udf = udf(categorize_amount, StringType())

# Use it
df_categorized = df_sales.withColumn("amount_category", categorize_udf(col("purchase_amount")))
```

### 6.2 Pandas UDFs (Vectorized — Much Faster)

Pandas UDFs operate on entire columns (as Pandas Series) using Apache Arrow for serialization, making them **significantly faster** than row-at-a-time UDFs.

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

# Scalar pandas UDF — operates on a Series, returns a Series
@pandas_udf(StringType())
def categorize_vectorized(amounts: pd.Series) -> pd.Series:
    def categorize(amount):
        if pd.isna(amount):
            return "UNKNOWN"
        if amount > 10000:
            return "HIGH"
        elif amount > 1000:
            return "MEDIUM"
        return "LOW"
    return amounts.apply(categorize)

df_categorized = df_sales.withColumn(
    "amount_category",
    categorize_vectorized(col("purchase_amount"))
)
```

### 6.3 When to Use UDFs vs Built-in Functions

| Situation | Recommendation |
|-----------|---------------|
| Logic is achievable with `when`, `col`, string functions etc. | Always prefer built-in functions |
| Complex custom logic, simple Python | Pandas UDF (vectorized) |
| Logic requires external libraries or stateful logic | Regular UDF (accept the slowness) |

---

## Chapter 7: Structured Streaming {#chapter-7}

Spark is not just for batch processing. **Structured Streaming** lets you apply DataFrame transformations to an infinite, continuously-arriving stream of data (like Kafka events) using the exact same API.

### 7.1 Reading from a Stream

```python
# Read from Apache Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "sales_events") \
    .load()

# Kafka gives you raw bytes — parse the value as JSON
from pyspark.sql.functions import from_json, col

df_parsed_stream = df_stream.select(
    from_json(col("value").cast("string"), sales_schema).alias("data")
).select("data.*")
```

### 7.2 Stateless Stream Transformations

Any standard transformation that doesn't require remembering past data works out of the box.

```python
# Filter and transform the live stream exactly like a batch DataFrame
df_live_high_value = df_parsed_stream \
    .filter(col("purchase_amount") > 500) \
    .withColumn("tax", col("purchase_amount") * 0.08)
```

### 7.3 Stateful Operations: Windowed Aggregations

> **Theory:** To aggregate streaming data (e.g., total sales per minute), you define a **time window**. Spark remembers state across micro-batches to produce correct aggregates.

```python
from pyspark.sql.functions import window

# Tumbling window: non-overlapping 5-minute buckets
df_windowed = df_parsed_stream \
    .groupBy(
        window(col("event_timestamp"), "5 minutes"),
        col("region")
    ) \
    .agg(sum("purchase_amount").alias("revenue_per_5min"))

# Sliding window: 10-minute window that moves every 5 minutes
df_sliding = df_parsed_stream \
    .groupBy(
        window(col("event_timestamp"), "10 minutes", "5 minutes"),
        col("region")
    ) \
    .agg(sum("purchase_amount").alias("revenue"))
```

### 7.4 Writing a Stream

```python
# Write aggregated results to console (for debugging)
query = df_windowed.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

# Write to Parquet files (micro-batch append)
query = df_live_high_value.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "/tmp/checkpoints/sales") \
    .option("path", "/data/streaming_output/") \
    .trigger(processingTime="1 minute") \
    .start()

# Keep stream running
query.awaitTermination()
```

### Output Modes

| Mode | Use when |
|------|----------|
| `append` | Only new rows are written; no aggregations (or append-safe aggregations) |
| `update` | Only changed rows since last trigger are written |
| `complete` | Entire result table is rewritten each trigger (use with aggregations only) |

---

## Chapter 8: Cluster Performance & Tuning {#chapter-8}

This is the "Zero to Hero" dividing line. Anyone can write a join. A data engineer knows how to make it run without failing with an Out Of Memory (OOM) error.

### 8.1 Caching

If your script uses the same DataFrame multiple times (e.g., you filter a dataset, write it to a database, and then run an aggregation on it), Spark will calculate that DataFrame **from scratch twice** due to lazy evaluation. Caching saves the intermediate state in RAM.

```python
from pyspark import StorageLevel

# Save DataFrame to Memory and Disk to prevent re-computation
df_heavy_processing.persist(StorageLevel.MEMORY_AND_DISK)

# ... perform multiple actions on df_heavy_processing ...

# Free up the RAM when done — always unpersist to avoid memory leaks
df_heavy_processing.unpersist()
```

### Storage Levels Comparison

| Level | Memory | Disk | CPU Cost | When to use |
|-------|--------|------|----------|-------------|
| `MEMORY_ONLY` | ✅ | ❌ | Low | DataFrame fits in RAM, used frequently |
| `MEMORY_AND_DISK` | ✅ | ✅ (overflow) | Medium | Default safe choice for most cases |
| `DISK_ONLY` | ❌ | ✅ | High | Very large data, used infrequently |

### 8.2 Partitioning Strategies

```python
# Check current partition count
print(df.rdd.getNumPartitions())

# Repartition: FULL shuffle, increases OR decreases partitions
# Use when: preparing for a groupBy/join, or partitioning by a key
df_repartitioned = df.repartition(100, col("region"))

# Coalesce: NO shuffle, only decreases partitions
# Use when: writing a small result to disk (avoids 200 tiny files)
df_small = df_filtered.coalesce(5)

# Write partitioned data (creates folders by value, like Hive partitioning)
df_sales.write.partitionBy("region", "purchase_year") \
    .mode("overwrite") \
    .parquet("path/partitioned_sales/")
```

> **When querying partitioned data**, Spark uses **partition pruning** — it reads only the folders it needs. A query filtered on `region = 'WEST'` skips 75% of the files.

### 8.3 Handling Data Skewness

> **Theory:** Data skew happens when one partition of data is massively larger than the others. For example, if you group sales by "City", New York will have millions of records while a small town has 10. The worker node handling New York will run out of memory and crash, while other worker nodes sit idle.

**Fix 1: Salting (Manual)**

```python
from pyspark.sql.functions import rand, monotonically_increasing_id

# Add a random "salt" prefix to the join key to spread out the hot partition
NUM_SALTS = 10

# On the large table: randomly assign a salt value
df_sales_salted = df_sales.withColumn(
    "salted_key",
    concat(col("customer_id"), lit("_"), (rand() * NUM_SALTS).cast("int").cast("string"))
)

# On the small/skewed reference table: explode to match all salt values
from pyspark.sql.functions import explode, array

df_customers_salted = df_customers.withColumn(
    "salt",
    explode(array([lit(str(i)) for i in range(NUM_SALTS)]))
).withColumn(
    "salted_key",
    concat(col("customer_id"), lit("_"), col("salt"))
)

# Now join on the salted key — no single node gets overloaded
df_result = df_sales_salted.join(df_customers_salted, "salted_key")
```

**Fix 2: Adaptive Query Execution (automatic — see below)**

### 8.4 Adaptive Query Execution (AQE)

In Spark 3.x, AQE is a game-changer. It allows Spark to analyze the data **during execution** and change its own physical plan on the fly. It can:
- Automatically coalesce small partitions after shuffles
- Switch standard joins to broadcast joins if a table gets heavily filtered
- Automatically handle skewed joins

```python
# Enable AQE in your Spark Session config
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

### 8.5 Reading the Execution Plan

Always check the physical plan for expensive operations before deploying.

```python
# See the query plan (start from the bottom, it reads upward)
df_joined.explain(mode="formatted")
```

Key things to look for in the plan:

| Symbol | Meaning | Good or Bad? |
|--------|---------|-------------|
| `BroadcastHashJoin` | Small table was broadcast | ✅ Good |
| `SortMergeJoin` | Both sides were sorted and shuffled | ⚠️ Expensive |
| `Exchange` | A shuffle happened | ⚠️ Check if avoidable |
| `FileScan` with `PartitionFilters` | Partition pruning is working | ✅ Good |

---

## Chapter 9: The Modern Lakehouse (Delta Lake) {#chapter-9}

Standard data lakes (saving Parquet files in S3/Cloud Storage) have a flaw: you cannot easily update or delete single records, and if a job fails halfway through, you end up with corrupted data.

**Delta Lake** is an open-source storage layer that brings ACID transactions to Spark.

### 9.1 Writing and Overwriting Delta

```python
# Write data in Delta format
df_sales.write.format("delta").mode("overwrite").save("/data/sales_table")

# Or create it as a managed SQL table
df_sales.write.format("delta").mode("overwrite").saveAsTable("sales")
```

### 9.2 Reading Delta

```python
# Read the current version
df_delta = spark.read.format("delta").load("/data/sales_table")

# Time Travel: Read a previous version
df_yesterday = spark.read.format("delta") \
    .option("versionAsOf", 5) \
    .load("/data/sales_table")

# Time Travel: Read data as of a specific timestamp
df_last_week = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-01") \
    .load("/data/sales_table")
```

### 9.3 Upserts (MERGE INTO)

With Delta, you can perform database-style "upserts" — inserting new records and updating existing ones in a single pass, with full ACID guarantees.

```python
from delta.tables import DeltaTable

# Load the target table
target_table = DeltaTable.forPath(spark, "/data/sales_table")

# Merge new daily data into the historical table
target_table.alias("target").merge(
    df_daily_updates.alias("source"),
    "target.sale_id = source.sale_id"
).whenMatchedUpdateAll(          # Update all columns when IDs match
).whenNotMatchedInsertAll(       # Insert new rows when no match found
).execute()
```

### 9.4 Deletes and Schema Evolution

```python
# Delete records (GDPR compliance use case)
target_table.delete("customer_id = 'CUST_12345'")

# Update specific records
from delta.tables import DeltaTable
target_table.update(
    condition="region = 'WEST'",
    set={"status": lit("REVIEWED")}
)

# Schema evolution: add new columns without rewriting the table
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
df_with_new_column.write.format("delta").option("mergeSchema", "true") \
    .mode("append").save("/data/sales_table")
```

### 9.5 Maintenance: OPTIMIZE and Z-ORDER

As you write data to Delta every day, you create thousands of tiny files. This "small file problem" destroys read performance. You must run optimization jobs to compact them.

```python
# OPTIMIZE (Bin-packing): Compacts small files into larger ~1GB files
spark.sql("OPTIMIZE delta.`/data/sales_table`")

# Z-Ordering: Co-locates related data (like customer_ids) in the same files.
# This allows Spark to skip reading 90% of the files when querying a specific customer.
spark.sql("OPTIMIZE delta.`/data/sales_table` ZORDER BY (customer_id)")

# VACUUM: Removes old files no longer needed by time travel
# WARNING: This makes time travel unavailable for the deleted versions
spark.sql("VACUUM delta.`/data/sales_table` RETAIN 168 HOURS")  # keep 7 days
```

### 9.6 Delta Table History

```python
# See all operations performed on the table
delta_table = DeltaTable.forPath(spark, "/data/sales_table")
delta_table.history().show(truncate=False)
```

---

## Chapter 10: Testing PySpark Code {#chapter-10}

Production pipelines **must** be tested. Bugs in data pipelines can silently corrupt months of data before anyone notices.

### 10.1 Setting Up a Test SparkSession

```python
# tests/conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for the test suite."""
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("pytest_spark") \
        .config("spark.sql.shuffle.partitions", "2")  # Keep it small for tests
        .getOrCreate()
    yield spark
    spark.stop()
```

### 10.2 Writing Unit Tests

```python
# tests/test_transformations.py
from pyspark.sql import Row
from jobs.transformations import add_tax_column

def test_add_tax_column(spark):
    # Arrange: Create a small test DataFrame
    data = [
        Row(sale_id=1, purchase_amount=100.0),
        Row(sale_id=2, purchase_amount=200.0),
    ]
    df_input = spark.createDataFrame(data)

    # Act: Run the transformation
    df_result = add_tax_column(df_input, tax_rate=0.1)

    # Assert: Verify the output
    results = df_result.collect()
    assert results[0]["tax_amount"] == 10.0
    assert results[1]["tax_amount"] == 20.0
    assert "tax_amount" in df_result.columns
```

### 10.3 Testing for Schema Correctness

```python
def test_output_schema(spark):
    df = spark.createDataFrame([Row(customer_id="C1", amount=100.0)])
    df_result = your_transformation(df)

    expected_columns = {"customer_id", "amount", "tax_amount", "status"}
    assert set(df_result.columns) == expected_columns
```

### 10.4 Testing for Data Quality

```python
def test_no_nulls_in_key_columns(spark):
    df_result = run_full_pipeline(spark)
    null_count = df_result.filter(col("customer_id").isNull()).count()
    assert null_count == 0, f"Found {null_count} null customer_ids!"

def test_no_negative_amounts(spark):
    df_result = run_full_pipeline(spark)
    negative_count = df_result.filter(col("purchase_amount") < 0).count()
    assert negative_count == 0
```

---

## Chapter 11: Best Practices Cheat Sheet {#chapter-11}

### Performance Checklist

| Checklist Item | Why it matters |
|---|---|
| ✅ Define schema explicitly | Avoid `inferSchema` overhead and type guessing |
| ✅ Filter early, select what you need | Reduces data shuffled and processed |
| ✅ Use Parquet/Delta over CSV/JSON | Columnar, compressed, schema-embedded |
| ✅ Broadcast small tables in joins | Eliminates shuffle on the large side |
| ✅ Enable AQE (`spark.sql.adaptive.enabled=true`) | Auto-handles skew and partition sizing |
| ✅ Check `.explain()` before deploying | Catch SortMergeJoins and unexpected shuffles |
| ✅ Cache DataFrames used multiple times | Prevents expensive recomputation |
| ✅ Always `unpersist()` when done | Prevents memory leaks in long jobs |
| ✅ Coalesce before writing small results | Avoids thousands of tiny output files |
| ✅ OPTIMIZE + Z-ORDER Delta tables regularly | Maintains query performance over time |

### Common Mistakes to Avoid

```python
# ❌ BAD: Using collect() on large DataFrames
# This pulls ALL data to the driver and will OOM crash it
all_data = df.collect()   # DON'T do this on big data

# ✅ GOOD: Use show() to preview, or write() to persist
df.show(20)
df.write.parquet(...)


# ❌ BAD: Calling count() in a loop
for region in regions:
    count = df.filter(col("region") == region).count()  # N full scans!

# ✅ GOOD: Do one groupBy aggregation
df.groupBy("region").count().show()


# ❌ BAD: Using Python UDFs when built-in functions exist
custom_upper_udf = udf(lambda x: x.upper(), StringType())

# ✅ GOOD: Use built-in functions (100x faster)
df.withColumn("name", upper(col("name")))


# ❌ BAD: Not specifying a shuffle partition count
# Default 200 partitions can be massively wrong for your data size
spark = SparkSession.builder.appName("job").getOrCreate()

# ✅ GOOD: Tune to your data size (target ~128MB per partition)
spark = SparkSession.builder \
    .appName("job") \
    .config("spark.sql.shuffle.partitions", "50") \  # adjust for your scale
    .getOrCreate()
```

### Quick Reference: Key Imports

```python
# Session
from pyspark.sql import SparkSession

# Types
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, BooleanType, DateType, TimestampType, ArrayType, MapType
)

# Functions (use these instead of UDFs whenever possible)
from pyspark.sql.functions import (
    col, lit, when, coalesce,
    upper, lower, trim, length, concat, concat_ws, regexp_replace, split,
    to_date, to_timestamp, date_format, datediff, year, month, dayofweek,
    sum, avg, count, countDistinct, max, min, collect_list,
    row_number, rank, dense_rank, lead, lag,
    explode, from_json, broadcast,
    current_date, current_timestamp
)

# Window
from pyspark.sql.window import Window

# Delta Lake
from delta.tables import DeltaTable

# Storage levels for caching
from pyspark import StorageLevel
```

---

*Happy engineering — may your shuffles be few and your partitions be balanced.*
