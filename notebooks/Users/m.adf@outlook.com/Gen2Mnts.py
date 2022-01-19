# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://con1@ms01.blob.core.windows.net",
  mount_point = "/mnt/con1",
  extra_configs = {"fs.azure.account.key.ms01.blob.core.windows.net": "CtDLcqJK9JnJz8Y1bZ+4xSx+imFoHivFRn8nBmHH328bzKqb/IZg/x8ezCCnVOteaXFNbcz4NlicV/jgp+x1eQ=="})

# COMMAND ----------

dbutils.fs.unmount("/mnt/con1")

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://con2@ms01.blob.core.windows.net",
  mount_point = "/mnt/con2",
  extra_configs = {"fs.azure.account.key.ms01.blob.core.windows.net": "CtDLcqJK9JnJz8Y1bZ+4xSx+imFoHivFRn8nBmHH328bzKqb/IZg/x8ezCCnVOteaXFNbcz4NlicV/jgp+x1eQ=="})

# COMMAND ----------

