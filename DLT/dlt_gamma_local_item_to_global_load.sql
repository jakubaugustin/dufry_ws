-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE gamma_local_item_to_local_bronze
AS SELECT *
  FROM cloud_files(
    "abfss://data@dufryworkshop.dfs.core.windows.net/GammaLocalItemToGlobal/",
    "csv",
    map("header", "true", "delimiter", ";", "inferSchema", "true")
  )

-- COMMAND ----------

CREATE LIVE TABLE gamma_local_item_to_local_silver(
    CONSTRAINT LOCAL_ITEM_CODE_not_empty EXPECT (`LOCAL_ITEM_CODE` IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT GLOBAL_ITEM_CODE_not_empty EXPECT (`GLOBAL_ITEM_CODE` IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT 
  * 
FROM 
  live.gamma_local_item_to_local_bronze
