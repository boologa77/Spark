# Databricks notebook source
# 🔥 Adaptive Query Execution (AQE) in PySpark - Interview Ready Answer 🔥
# 🔹 What is Adaptive Query Execution (AQE)?
# Adaptive Query Execution (AQE) is an optimization technique in Spark 3.0+ that dynamically changes the query execution plan at runtime based on actual data statistics.
# Unlike static query plans, AQE adjusts itself on the fly, making queries more efficient.
# 👉 Interview Explanation:

# "Adaptive Query Execution (AQE) in Spark optimizes queries at runtime based on real data instead of relying on static estimates. This improves performance by dynamically adjusting the execution plan, such as optimizing joins, rebalancing partitions, and reducing data shuffling."

# 🔹 How AQE Works (Key Optimizations)
# AQE monitors the execution and makes dynamic adjustments in three major ways:

# 1️⃣ Dynamic Join Selection (Switching Join Strategy)

# Spark chooses the best join type based on data sizes at runtime.
# Example: If one table is small, Spark switches from Sort-Merge Join to a faster Broadcast Join.
# 2️⃣ Skew Join Optimization

# If one partition is much larger than others, Spark splits the skewed partition into smaller chunks, reducing execution time.
# 3️⃣ Dynamic Partition Coalescing

# If too many small partitions exist, Spark merges them to reduce overhead.
# This prevents too many small files from slowing down execution.
# 👉 Interview Explanation:

# "AQE dynamically selects the best join strategy, fixes data skew issues by breaking large partitions, and merges small partitions to improve execution efficiency."

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('AQE').getOrCreate()

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled",True)

# COMMAND ----------


