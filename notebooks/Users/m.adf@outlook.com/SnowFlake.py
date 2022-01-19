# Databricks notebook source
# Use secrets DBUtil to get Snowflake credentials.
#dbutils.secrets.get("data-warehouse", "<snowflake-user>")
#dbutils.secrets.get("data-warehouse", "<snowflake-password>")

user = 'manadf'
password = '@Azureadf99'

# snowflake connection options
options = {
  "sfUrl": "lf60864.snowflakecomputing.com",
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "DB01",
  "sfSchema": "PUBLIC",
  "sfWarehouse": "SFWH1"
}

# COMMAND ----------

df = spark.read \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "EmpInfo") \
  .load()

display(df)

# COMMAND ----------

dataset = spark.read.format("snowflake").options(**options).option("query", "select * from EmpInfo").load()