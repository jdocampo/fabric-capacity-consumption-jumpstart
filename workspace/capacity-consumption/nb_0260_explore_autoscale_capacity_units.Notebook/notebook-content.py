# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7f45a2ed-125b-448d-bf75-2a9613526182",
# META       "default_lakehouse_name": "capacity_metrics",
# META       "default_lakehouse_workspace_id": "cb2325c3-50bc-40bf-91c8-49effb0200d8",
# META       "known_lakehouses": [
# META         {
# META           "id": "7f45a2ed-125b-448d-bf75-2a9613526182"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_0260_explore_autoscale_capacity_units
# 
# ## Purpose
# **Diagnostic / validation notebook.** Extends 0240 with **Autoscale + Spark** metrics,
# including cumulative CU consumption analysis across operations.
# 
# > **READ-ONLY** — this notebook does NOT write to any table.
# 
# ## Inputs
# - `gold.capacities` — capacity list with region
# - `gold.capacity_metrics_by_timepoint` — standard CU metrics
# - `gold.timepoint_metrics` — standard activity detail
# - `gold.timepoint_metrics_autoscale` — autoscale activity detail with `Acc_CU_s` (cumulative CU)
# 
# ## Key Autoscale Metrics
# - `SumCU_s` — **point-in-time** CU for that operation at that timepoint
# - `Acc_CU_s` — **cumulative total** CU for that operation up to and including this timepoint
# - `Timepoint_workload_autoscale_limit` — the autoscale CU limit at that moment
# - To get the **total cumulative CU at the latest timepoint** per operation:
#   query `MAX(Acc_CU_s)` grouped by `CapacityId, Operation_Id`
# 
# ## Execution Order
# Run **after** nb_0300_generate_gold_tables. No side effects.
# 
# ## Usage
# 1. Set `start_date` and `end_date`.
# 2. Optionally set `filter_operation_id` to drill into a specific operation.
# 3. Run all cells for full autoscale cost and cumulative CU analysis.


# CELL ********************

from pyspark.sql.functions import col, to_timestamp, to_date, max as spark_max
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('nb_0250')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run "./nb_0110_shared_config"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# Diagnostic parameters
# ---------------------------------------------------------------------------
start_date = "04 June 2026 00:00"
end_date   = "05 June 2026 00:00"
filter_capacity_id  = TARGET_CAPACITY_ID  # None for all
filter_operation_id = None                 # set to a specific Operation_Id to drill in
pricing_type = 'PAYG'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pricing_data = [
    ('North Europe',   0.380, 'PAYG', 'USD'),
    ('West Europe',    0.380, 'PAYG', 'USD'),
    ('East US',        0.360, 'PAYG', 'USD'),
    ('East US 2',      0.360, 'PAYG', 'USD'),
    ('West US',        0.380, 'PAYG', 'USD'),
    ('Southeast Asia', 0.400, 'PAYG', 'USD'),
    ('Australia East', 0.430, 'PAYG', 'USD'),
    ('North Europe',   0.250, 'Reservation', 'USD'),
    ('West Europe',    0.250, 'Reservation', 'USD'),
    ('East US',        0.235, 'Reservation', 'USD'),
]
pricing_df = spark.createDataFrame(pricing_data, ['Region','CU_price_per_hour','Type','Currency'])
pricing_df = pricing_df.filter(col('Type') == pricing_type)

cap_query = f"""
    SELECT CapacityId,
           CASE Region WHEN 'Default' THEN 'West Europe' ELSE Region END AS Region,
           CapacityName, state
    FROM {GOLD_CAPACITIES}
"""
cap_regions = spark.sql(cap_query)
if filter_capacity_id:
    cap_regions = cap_regions.filter(col('CapacityId') == filter_capacity_id.upper())
capacity_with_pricing = cap_regions.join(pricing_df, 'Region', 'left')
logger.info('Setup complete: %d capacities loaded', capacity_with_pricing.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------
# Standard CU summary (from gold.capacity_metrics_by_timepoint)
# -----------------------------------------------------------------------
cov_q = f"""
    SELECT to_date(TimePoint) AS Day, COUNT(*) AS Timepoints,
           AVG(TotalCUUsagePercentage) AS AvgCUUsage_pct,
           MAX(TotalCUUsagePercentage) AS PeakCUUsage_pct
    FROM {GOLD_METRICS_BY_TIMEPOINT}
    WHERE TimePoint BETWEEN to_timestamp('{start_date}', 'dd MMMM yyyy HH:mm')
                       AND to_timestamp('{end_date}', 'dd MMMM yyyy HH:mm')
"""
if filter_capacity_id:
    cov_q += f" AND CapacityId = '{filter_capacity_id.upper()}'"
cov_q += ' GROUP BY to_date(TimePoint) ORDER BY 1'
logger.info('Standard CU summary:')
display(spark.sql(cov_q))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------
# Autoscale activity detail with cumulative CU
# gold.timepoint_metrics_autoscale already contains Acc_CU_s (from 0300)
# -----------------------------------------------------------------------
as_q = f"""
    SELECT
        Workspace_name, Item_name, Unique_key, Operation,
        Operation_Id, Operation_start_time, Status, User, Item_kind,
        Billing_type, Timepoint, CapacityId,
        SumCU_s,
        Acc_CU_s,
        Timepoint_workload_autoscale_limit
    FROM {GOLD_TP_METRICS_AS}
    WHERE Timepoint BETWEEN to_timestamp('{start_date}', 'dd MMMM yyyy HH:mm')
                       AND to_timestamp('{end_date}', 'dd MMMM yyyy HH:mm')
"""
if filter_capacity_id:
    as_q += f" AND CapacityId = '{filter_capacity_id.upper()}'"
if filter_operation_id:
    as_q += f" AND Operation_Id = '{filter_operation_id}'"
autoscale_df = spark.sql(as_q)
logger.info('Autoscale activities: %d rows', autoscale_df.count())
display(autoscale_df.orderBy('Operation_Id','Timepoint'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------
# Cumulative CU summary: total CU consumed per operation by end of range
# MAX(Acc_CU_s) gives the cumulative total at the latest timepoint
# -----------------------------------------------------------------------
cum_summary = autoscale_df.groupBy('CapacityId','Operation_Id','Workspace_name','Item_name','Operation').agg(
    spark_max('Acc_CU_s').alias('CumulativeCU_s'),
    spark_max('SumCU_s').alias('PeakTimepointCU_s'),
    spark_max('Timepoint_workload_autoscale_limit').alias('AutoscaleLimit')
).withColumn('CumulativeCU_hours', col('CumulativeCU_s') / 3600.0)
logger.info('Cumulative CU per operation:')
display(cum_summary.orderBy('CumulativeCU_s', ascending=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------
# Autoscale cost estimate
# -----------------------------------------------------------------------
as_priced = autoscale_df.join(capacity_with_pricing.select('CapacityId','Region','CU_price_per_hour'), 'CapacityId', 'left')
as_cost = as_priced.withColumn('TimepointCost_USD', (col('SumCU_s') / 3600.0) * col('CU_price_per_hour'))
as_cost_summary = as_cost.groupBy('Workspace_name','Item_name','Operation','Billing_type','CapacityId').agg(
    {'SumCU_s':'sum','TimepointCost_USD':'sum','Operation_Id':'count_distinct'}
).withColumnRenamed('sum(SumCU_s)','TotalCU_s'
).withColumnRenamed('sum(TimepointCost_USD)','TotalCost_USD'
).withColumnRenamed('countDistinct(Operation_Id)','UniqueOperations')
logger.info('Autoscale cost summary:')
display(as_cost_summary.orderBy('TotalCost_USD', ascending=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }


