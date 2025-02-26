# Databricks notebook source
data = [
    ("Alice", ["Refrigerator", "Microwave", "Washing Machine"]),
    ("Bob", ["Vacuum Cleaner", "Air Conditioner", None]),
    ("Charlie", ["Dishwasher", "Toaster", "Ceiling Fan"]),
    ("David", ["Electric Kettle", None, "Oven"]),
    ("Emma", [None])
]

df=spark.createDataFrame(data=data,schema=["name","Appliances"])
df.show()
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import explode_outer,posexplode_outer

# COMMAND ----------

df5 = df.select("name", posexplode_outer("Appliances")).show()

# COMMAND ----------

data = [
    ("Alice", {"TV": "LG", "Fridge": "Samsung", "Washing Machine": "Bosch"}),
    ("Bob" ,{"Vacuum Cleaner": "Dyson", "Air Conditioner": "Daikin", "Television": "Sony"}),
    ("Charlie" ,{"Dishwasher": "Whirlpool", "Toaster": "Philips", "Ceiling Fan": "Havells"}),
    ("David", {"Electric Kettle": "Breville", "Water Purifier": "Kent", "Oven": "Samsung"}),
    ("Emma", None)
]

df1=spark.createDataFrame(data=data,schema=['name','Appliance'])
df1.show()
df1.printSchema()


# COMMAND ----------

from pyspark.sql.functions import explode

df2=df1.select(df1.name,explode(df1.Appliance))
df2.printSchema()
display(df2)


# COMMAND ----------

df3=df.select(df.name,explode(df.Appliances))
df3.printSchema()
display(df3)

# COMMAND ----------

df3.filter(df3.col.isNotNull()).show()

# COMMAND ----------


