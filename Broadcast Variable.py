# Databricks notebook source
# MAGIC %md
# MAGIC # Definition for interview about broadcast variable

# COMMAND ----------

#  Broadcast Join in PySpark - Interview Ready Answer 🔥
# 🔹 What is Broadcast in PySpark?
# Broadcasting in PySpark is an optimization technique that sends a small DataFrame to all nodes in a cluster, instead of shuffling large amounts of data.
# It is mainly used in Broadcast Joins, where a small DataFrame is replicated across worker nodes to speed up join operations.
# 🔹 When is Broadcast Join Used?
# When one table is much smaller than the other.
# When reducing network data shuffle is needed.
# When the smaller table fits in memory.
# 👉 Interview Explanation:

# "A Broadcast Join in PySpark improves performance by sending a small table to all worker nodes, avoiding expensive data shuffles. This is useful when one table is small and the other is large."

# COMMAND ----------

# MAGIC %md
# MAGIC #Creating a dataframe

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark Session
spark = SparkSession.builder.appName("FactDimensionTables").getOrCreate()

# Define Schema for Fact Table (Sales Data)
fact_schema = StructType([
    StructField("store_id", IntegerType(), True),
    StructField("item_name", StringType(), True),
    StructField("amount", IntegerType(), True)
])

# Create Fact Table (Sales Data)
fact_sales = spark.createDataFrame([
    (101, "Laptop", 1500),
    (102, "Mobile", 800),
    (103, "Tablet", 500),
    (101, "Headphones", 200),
    (104, "Smartwatch", 300),
    (102, "Laptop", 1200),
    (103, "Mobile", 900),
    (104, "Tablet", 600),
    (101, "Smartwatch", 350),
    (103, "Laptop", 1400)
], schema=fact_schema)

# Define Schema for Dimension Table (Store Details)
dim_schema = StructType([
    StructField("store_id", IntegerType(), True),
    StructField("store_name", StringType(), True)
])

# Create Dimension Table (Store Details)
dim_store = spark.createDataFrame([
    (101, "Tech Hub"),
    (102, "Gadget World"),
    (103, "Mobile Market"),
    (104, "Digital Store")
], schema=dim_schema)


# COMMAND ----------

df=fact_sales.join(dim_store,fact_sales.store_id == dim_store.store_id,"outer").select(fact_sales.store_id,fact_sales.item_name,dim_store.store_name)
display(df)

# COMMAND ----------

from pyspark.sql.functions import broadcast

# COMMAND ----------

df = fact_sales.join(broadcast(dim_store),fact_sales['store_id']==dim_store['store_id']).select(fact_sales.store_id,fact_sales.item_name,dim_store.store_name)
display(df)

# COMMAND ----------

df.explain(True)
