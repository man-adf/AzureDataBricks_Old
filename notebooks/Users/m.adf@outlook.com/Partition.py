# Databricks notebook source
from pyspark.sql import*
from datetime import*
from databricks import*
from pyspark.sql.types import*
from pyspark.sql.functions import*

# COMMAND ----------

# Create the JDBC URL using Python

jdbcHostname = "asql01.database.windows.net"
jdbcDatabase = "DB01"
jdbcPort = 1433

url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

properties = {
  "user" : "asql01",
  "password" : "@Azureadf99",
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

df = spark.read.jdbc(url=url, table="Partition", properties=properties).orderBy("Salary")

# COMMAND ----------

df.display()

# COMMAND ----------

Wind = Window.partitionBy("Dept").orderBy("Salary")

# COMMAND ----------

df1 = df.withColumn("Avg", avg(col("Salary")).over(Wind)).withColumn("Sum", sum(col("Salary")).over(Wind)).withColumn("Min", min(col("Salary")).over(Wind)).withColumn("Max", max(col("Salary")).over(Wind))

# COMMAND ----------

df1.display()

# COMMAND ----------

df2 = df.withColumn("Rank",rank().over(Wind)).withColumn("DenseRank",dense_rank().over(Wind)).withColumn("RowNum",row_number().over(Wind)).withColumn("PercentRank",percent_rank().over(Wind)).withColumn("CumulativeDist",cume_dist().over(Wind)).withColumn("nTile",ntile(2).over(Wind))

# COMMAND ----------

df2.display()

# COMMAND ----------

df2.write.jdbc(url=url, table="PartBySpark", mode="overwrite", properties=properties)

# COMMAND ----------

