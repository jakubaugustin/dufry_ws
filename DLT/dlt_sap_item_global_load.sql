-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE sap_item_global_bronze
AS SELECT *
  FROM cloud_files(
    "abfss://data@dufryworkshop.dfs.core.windows.net/SapItemGlobal/",
    "csv",
    map("header", "true", "delimiter", ";", "inferSchema", "true")
  )

-- COMMAND ----------

CREATE LIVE TABLE sap_item_global_silver
COMMENT "Cleansed SAP item global data"
AS SELECT
  si.`SAPItemCode` AS `GlobalItemCode`
  , si.`SAPItemDescription`
  , si.`SAPDufryCategoryCode`
  , si.`SAPDufryCategoryDesc`
  , si.`SAPDufryBrandCode`
  , si.`SAPDufryBrandDesc`
FROM 
  live.sap_item_global_bronze si

-- COMMAND ----------

CREATE LIVE TABLE sap_item_global_gold
COMMENT "Cleansed SAP item global data"
AS SELECT * FROM 
live.sap_item_global_silver
