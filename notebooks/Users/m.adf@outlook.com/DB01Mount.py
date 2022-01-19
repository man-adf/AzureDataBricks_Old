# Databricks notebook source
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

df = spark.read.format("csv").options(header='true', inferSchema='true').load("/mnt/con1/Bronze/SalesOrderHeader.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.jdbc(url=url, table="SOH", mode="overwrite", properties=properties)

# COMMAND ----------

