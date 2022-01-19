# Databricks notebook source
from pyspark.sql.functions import*
from datetime import*
from databricks import*
from pyspark.sql.types import*
from pyspark.sql.window import*

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

Wind = Window.partitionBy("Dept").orderBy("salary")

# COMMAND ----------

df = df.withColumn("Rank",rank().over(Wind)).withColumn("DenseRank",dense_rank().over(Wind)).withColumn("RowNum",row_number().over(Wind)).withColumn("PercentRank",percent_rank().over(Wind)).withColumn("CumulativeDist",cume_dist().over(Wind)).withColumn("nTile",ntile(2).over(Wind))

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.jdbc(url=url, table="PartBySpark", mode="overwrite", properties=properties)

# COMMAND ----------

