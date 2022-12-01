# Databricks notebook source
# DBTITLE 1,Check folder exists in ADSL - using Python
dbutils.fs.ls("abfss://data@dufryworkshop.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls abfss://data@dufryworkshop.dfs.core.windows.net/

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls abfss://data@dufryworkshop.dfs.core.windows.net/gamma

# COMMAND ----------

data = spark \
    .read \
    .format('csv') \
    .option('delimiter', ';') \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .load('abfss://data@dufryworkshop.dfs.core.windows.net/gamma/GammaExport_20221114_20221120.csv')

# COMMAND ----------

display(data.dtypes)

# COMMAND ----------

data.display()

# COMMAND ----------


