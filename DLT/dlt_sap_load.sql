-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE sap_bronze
AS SELECT *
  FROM cloud_files(
    "abfss://data@dufryworkshop.dfs.core.windows.net/sap/",
    "csv",
    map("header", "true", "delimiter", ";", "inferSchema", "true")
  )

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sap_silver
AS SELECT
TO_DATE(s.`SALES_DATE`, 'yyyyMMdd') as `SALES_DATE`,
s.`SALES_TIME`,
s.`COMP_CODE`,
s.`STORE_CODE`,
s.`TICKET_NUMBER`,
s.`TICKET_LN_NUMBER`,
s.`ITEM_CODE`,
s.`PAX_NATIONALITY`,
s.`PAX_DESTINATION`,
s.`AIRLINE_CODE`,
s.`FLIGHT_CODE`,
s.`PAX_GENDER`,
s.`POS_CURR`,
s.`TAX_STATUS_CODE`,
s.`PROMO_CODE`,
s.`RED_LOYALTY`,
s.`QTY_SOLD_BUOM`,
s.`NET_AMNT`,
s.`VAT_AMNT`,
s.`LINE_DISC_AMNT`,
s.`ITEM_COGS_LCY`,
s.`NON_VAT_TAX_AMNT`,
s.`ISSUED_VOUCH`,
s.`REDEEMED_VOUCH`
FROM
  STREAM(live.sap_bronze) s

-- COMMAND ----------

CREATE STREAMING LIVE TABLE sap_transactions_silver
AS SELECT

  s.`SALES_DATE` AS `SALES_DATE`
  , s.`SALES_TIME` AS `SALES_TIME`
  , s.`COMP_CODE` AS `COMP_CODE`
  , s.`TICKET_NUMBER` AS `RECEIPT_NUMBER`
  , s.`TICKET_LN_NUMBER` AS `ITEM_LN_NUMBER`
  , s.`ITEM_CODE` AS `ITEM_CODE`
  , s.`PAX_NATIONALITY` AS `PAX_NATIONALITY`
  , s.`PAX_DESTINATION` AS `PAX_DESTINATION`
  , s.`AIRLINE_CODE` AS `AIRLINE_CODE`
  , s.`FLIGHT_CODE` AS `FLIGHT_CODE`
  , s.`PAX_GENDER` AS `PAX_GENDER`
  , 'SAP' AS `SOURCE_SYSTEM`

FROM
  STREAM(live.sap_silver) s
