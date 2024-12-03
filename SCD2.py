# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

customer_dim_data = [

(1,'manish','arwal','india','N','2022-09-15','2022-09-25'),
(2,'vikash','patna','india','Y','2023-08-12',None),
(3,'nikita','delhi','india','Y','2023-09-10',None),
(4,'rakesh','jaipur','india','Y','2023-06-10',None),
(5,'ayush','NY','USA','Y','2023-06-10',None),
(1,'manish','gurgaon','india','Y','2022-09-25',None),
]

customer_schema= ['id','name','city','country','active','effective_start_date','effective_end_date']

customer_dim_df = spark.createDataFrame(data= customer_dim_data,schema=customer_schema)


# COMMAND ----------

customer_dim_df.show()

# COMMAND ----------

sales_data = [

(1,1,'manish','2023-01-16','gurgaon','india',380),
(77,1,'manish','2023-03-11','bangalore','india',300),
(12,3,'nikita','2023-09-20','delhi','india',127),
(54,4,'rakesh','2023-08-10','jaipur','india',321),
(65,5,'ayush','2023-09-07','mosco','russia',765),
(89,6,'rajat','2023-08-10','jaipur','india',321)
]

sales_schema = ['sales_id', 'customer_id','customer_name', 'sales_date', 'food_delivery_address','food_delivery_country', 'food_cost']

sales_df = spark.createDataFrame(data=sales_data,schema=sales_schema)

# COMMAND ----------

sales_df.show()

# COMMAND ----------

joined_df = customer_dim_df.join(sales_df, customer_dim_df.id == sales_df.customer_id,"left")

# COMMAND ----------

display(joined_df)

# COMMAND ----------

new_records_df = joined_df.where(
                    (col("food_delivery_address") != col("city")) & (col("active") == "Y"))\
                    .withColumn("active", lit("Y"))\
                    .withColumn("effective_start_date", col("sales_date"))\
                    .withColumn("effective_end_date", lit(None))\
                        .select(
                        "customer_id",
                        "customer_name",
                        col("food_delivery_address").alias("city"),
                        "food_delivery_country",
                        "active",
                        "effective_start_date",
                        "effective_end_date"
                    )
new_records_df.show()

# COMMAND ----------

old_records = joined_df.where(
        (col("food_delivery_address") != col("city")) & (col("active") == "Y"))\
        .withColumn("active", lit("N"))\
        .withColumn("effective_end_date", col("sales_date"))\
        .select(
            "customer_id",
            "customer_name",
            "city",
            "country",
            "active",
            "effective_start_date",
            "effective_end_date"
)
old_records.show()


# COMMAND ----------

new_customers = sales_df.join(
                    customer_dim_df, sales_df.customer_id == customer_dim_df.id, "leftanti")\
                    .withColumn("active", lit("Y"))\
                    .withColumn("effective_start_date", col("sales_date"))\
                    .withColumn("effective_end_date", lit(None))\
                        .select(
                        "customer_id",
                        "customer_name",
                        col("food_delivery_address").alias("city"),
                        "food_delivery_country",
                        "active",
                        "effective_start_date",
                        "effective_end_date"
                    )
new_customers.show()

# COMMAND ----------

final_records = customer_dim_df.union(new_records_df).union(old_records).union(new_customers)
final_records.show()

# COMMAND ----------

window = Window.partitionBy("id","active").orderBy(col("effective_start_date").desc())
final_records.withColumn("rank", rank().over(window))\
                .filter(~((col("active") == "Y") & (col("rank") >= 2))).drop("rank").show()
