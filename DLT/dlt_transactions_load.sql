-- Databricks notebook source
CREATE STREAMING LIVE TABLE transactions_silver
AS SELECT
  *
FROM
  STREAM(live.sap_transactions_silver)
UNION ALL
SELECT
  *
FROM
  STREAM(live.gamma_transactions_silver)

-- COMMAND ----------

CREATE LIVE TABLE transactions_gold
AS
SELECT 
    t.*
    , dayofweek(t.`SALES_DATE`) AS `dayOfWeek`
    , count(*) OVER (PARTITION BY t.`RECEIPT_NUMBER`) AS `itemsBought`
    , i.*

FROM live.transactions_silver t
LEFT JOIN live.sap_item_global_silver i
ON t.item_code = i.GlobalItemCode
