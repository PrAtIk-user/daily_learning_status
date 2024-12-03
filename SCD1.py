# Databricks notebook source
dbutils.fs.mkdirs("/FileStore/scd_req_dt/")

# COMMAND ----------

initial_data = [
    {"customer_id": 1, "name": "John Doe", "email": "john@example.com", "address": "123 Main St"},
    {"customer_id": 2, "name": "Jane Smith", "email": "jane@example.com", "address": "456 Elm St"},
    {"customer_id": 3, "name": "Alice Brown", "email": "alice@example.com", "address": "789 Pine St"},
]
initial_df = spark.createDataFrame(initial_data)

initial_df.show()

# COMMAND ----------



initial_df.write.format("delta").mode("overwrite").save("/FileStore/scd_req_dt/customer_target")

# COMMAND ----------

updated_data = [
    {"customer_id": 1, "name": "John Doe", "email": "john_new@example.com", "address": "123 Main St"},
    {"customer_id": 2, "name": "Jane Smith", "email": "jane@example.com", "address": "456 Maple St"},
    {"customer_id": 4, "name": "Bob White", "email": "bob@example.com", "address": "101 Oak St"},
]
updated_df = spark.createDataFrame(updated_data)

updated_df.show()

# COMMAND ----------



updated_df.write.format("delta").mode("overwrite").save("/FileStore/scd_req_dt/customer_source")

# COMMAND ----------

from delta.tables import DeltaTable

source_path = "/FileStore/scd_req_dt/customer_source"
target_path = "/FileStore/scd_req_dt/customer_target"

source_df = spark.read.format("delta").load(source_path)
delta_table = DeltaTable.forPath(spark, target_path)

delta_table.alias("tgt").merge(
    source_df.alias("src"),
    "tgt.customer_id = src.customer_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

spark.read.format("delta").load(target_path).show()
