# Databricks notebook source
from databricks.feature_store import feature_table
import pyspark.pandas as ps

# COMMAND ----------

from pyspark.sql import functions as F

gamma = spark.table('dufry_data_load.gamma_silver_merge')
items = spark.table('dufry_data_load.sap_item_global_silver')

data = gamma \
    .join(items, how='left', on=gamma['GLOBAL_ITEM_CODE'] == items['GlobalItemCode']) \
    .withColumn('DAY_OF_WEEK', F.dayofweek(gamma['REAL_DATE_OF_SALE'])) \
    .withColumn('RECEIPT_ID', F.sha2(F.concat(
        gamma['REAL_DATE_OF_SALE'], F.lit('_'),
        gamma['TIME_OF_SALE'], F.lit('_'),
        gamma['GLOBAL_COMPANY_ID'], F.lit('_'),
        gamma['LOCAL_STORE_CODE'], F.lit('_'),
        gamma['SALES_RECEIPT_NUMBER']), 256))

# COMMAND ----------

data.display()

# COMMAND ----------

import re

def compute_features(data):
  
  # Convert to koalas
  data = data.pandas_api()
  
  # OHE
  data = ps.get_dummies(data, 
                        columns=['SAPDufryCategoryDesc'], dtype = 'int64')

#      'NATIONALITY_CODE': 'first',
#      'AIRLINE_CODE': 'first',

  data = data.groupby("RECEIPT_ID").aggregate({
      'RECEIPT_ID': 'first',
      'GENDER_CODE': 'first',
      'DAY_OF_WEEK': 'first',
      'SALES_RECEIPT_LINE': 'max',
      'SAPDufryCategoryDesc_Alcoholic Beverages': 'max'
  })

  # OHE 2
  data = ps.get_dummies(data, 
                        columns=['GENDER_CODE', 'dayOfWeek'], dtype = 'int64')
    
  # Rename featrue column 
  # data = data.rename({'SAPDufryCategoryDesc_Alcoholic Beverages': 'bought_alcohol'})

  # Clean up column names
  data.columns = [re.sub(r'[\(\)]', ' ', name).lower() for name in data.columns]
  data.columns = [re.sub(r'[ -]', '_', name).lower() for name in data.columns]

  # Drop missing values
  data = data.dropna()
  
  return data

# COMMAND ----------

features_df = compute_features(data)

# COMMAND ----------

for f in features_df.columns:
    print(f)

# COMMAND ----------

features_df.display()

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

try:
  #drop table if exists
  fs.drop_table(f'dufry_data_load.model_alcohol_bev')
except:
  pass
#Note: You might need to delete the FS table using the UI
feature_table = fs.create_table(
  name=f'dufry_data_load.model_alcohol_bev',
  primary_keys='receipt_id',
  schema=features_df.spark.schema(),
  description='Feature table'
)

fs.write_table(df=features_df.to_spark().limit(100000), name=f'dufry_data_load.model_alcohol_bev', mode='overwrite')
