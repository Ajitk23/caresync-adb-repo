# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # NB_lab_test_trends
# MAGIC Builds `adb_caresync.gold.lab_test_trends` per STTM *Silver to Gold v1.0* (sheet `lab_test_trends`).
# MAGIC
# MAGIC * **Grain:** one row per test type (`test_name`) per calendar month
# MAGIC * **Driving table:** `adb_caresync.silver.lab_results` grouped by `test_name`, `year(test_date)`, `month(test_date)`
# MAGIC * **Joins (LEFT, optional):** `calendar` on `date_key` of the first day of the month
# MAGIC * **Window:** `LAG(abnormal_rate_pct)` partitioned by `test_name`, ordered by `year, month`
# MAGIC * **Filters:** none
# MAGIC * **Load:** FULL rebuild via `CREATE OR REPLACE TABLE ... AS SELECT`

# COMMAND ----------

# DBTITLE 1,Create gold schema
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS adb_caresync.gold;

# COMMAND ----------

# DBTITLE 1,Build gold.lab_test_trends
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE adb_caresync.gold.lab_test_trends
# MAGIC COMMENT 'Gold: one row per test type per month with month-over-month abnormal-rate trend (STTM Silver to Gold v1.0)'
# MAGIC AS
# MAGIC WITH monthly AS (
# MAGIC   -- Lab results aggregated to test_name x month grain
# MAGIC   SELECT
# MAGIC     test_name,
# MAGIC     year(test_date)                       AS year,
# MAGIC     month(test_date)                      AS month,
# MAGIC     COUNT(*)                              AS total_tests,
# MAGIC     COUNT_IF(result_status = 'Abnormal')  AS abnormal_test_count,
# MAGIC     COUNT_IF(result_status = 'Critical')  AS critical_test_count
# MAGIC   FROM adb_caresync.silver.lab_results
# MAGIC   GROUP BY test_name, year(test_date), month(test_date)
# MAGIC ),
# MAGIC rates AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     CAST(abnormal_test_count * 100.0 / NULLIF(total_tests, 0) AS DECIMAL(5,2)) AS abnormal_rate_pct,
# MAGIC     CAST(critical_test_count * 100.0 / NULLIF(total_tests, 0) AS DECIMAL(5,2)) AS critical_rate_pct
# MAGIC   FROM monthly
# MAGIC ),
# MAGIC with_lag AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     LAG(abnormal_rate_pct) OVER (PARTITION BY test_name ORDER BY year, month) AS prev_month_abnormal_rate_pct
# MAGIC   FROM rates
# MAGIC )
# MAGIC SELECT
# MAGIC   m.test_name,
# MAGIC   CAST(m.year AS INT)                                                     AS year,
# MAGIC   CAST(m.month AS INT)                                                    AS month,
# MAGIC   c.month_name,
# MAGIC   c.quarter_name,
# MAGIC   CAST(m.total_tests AS INT)                                              AS total_tests,
# MAGIC   CAST(m.abnormal_test_count AS INT)                                      AS abnormal_test_count,
# MAGIC   CAST(m.critical_test_count AS INT)                                      AS critical_test_count,
# MAGIC   m.abnormal_rate_pct,
# MAGIC   m.critical_rate_pct,
# MAGIC   CAST(m.prev_month_abnormal_rate_pct AS DECIMAL(5,2))                    AS prev_month_abnormal_rate_pct,  -- NULL for a test's first month
# MAGIC   CAST(m.abnormal_rate_pct - m.prev_month_abnormal_rate_pct AS DECIMAL(6,2)) AS abnormal_rate_change_pct    -- percentage-point change
# MAGIC FROM with_lag m
# MAGIC LEFT JOIN adb_caresync.silver.calendar c
# MAGIC   ON c.date_key = CAST(date_format(make_date(m.year, m.month, 1), 'yyyyMMdd') AS INT);

# COMMAND ----------

# DBTITLE 1,Return Record Count
source_table = "adb_caresync.silver.lab_results"
target_table = "adb_caresync.gold.lab_test_trends"

record_count = spark.table(source_table).count()
target_count = spark.table(target_table).count()
print(f"Step 6: Load finished. Source records processed: {record_count}, total records in {target_table}: {target_count}")
dbutils.notebook.exit(str(record_count))