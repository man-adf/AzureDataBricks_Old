# Databricks notebook source
from pyspark.sql import*
from datetime import*
from delta.tables import *
from databricks import*
from pyspark.sql.types import*
from pyspark.sql.functions import*

# COMMAND ----------

df = spark.read.format("parquet").load("/mnt/con1/Delta/ParquetTest.parquet")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/con1/Delta/SOH`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE salesaw.SOH USING DELTA LOCATION '/mnt/con1/Delta/SOH'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/con1/Delta/ParquetTest`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE salesaw.ParquetTest USING DELTA LOCATION '/mnt/con1/Delta/ParquetTest'

# COMMAND ----------

