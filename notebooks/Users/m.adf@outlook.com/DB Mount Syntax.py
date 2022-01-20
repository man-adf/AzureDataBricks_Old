# Databricks notebook source
# Create the JDBC URL using Python

jdbcHostname = "SERVER NAME"
jdbcDatabase = "DATABASE NAME"
jdbcPort = 1433

url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

properties = {
  "user" : "SQL USER ID",
  "password" : "SQL PASSWORD",
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

