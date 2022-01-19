# Databricks notebook source
from pyspark.sql.functions import*
from datetime import*
from databricks import*
from pyspark.sql.types import*
from pyspark.sql.functions import*

# COMMAND ----------

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]



# COMMAND ----------

