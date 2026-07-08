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

# # nb_0250_explore_capacity_units
# 
# ## Purpose
# **Diagnostic / validation notebook.** Reads gold tables to explore standard capacity
# CU consumption, estimate PAYG costs, and validate collection quality.
# 
# > **READ-ONLY** — this notebook does NOT write to any table.
# 
# ## Inputs
# - `gold.capacities` — capacity list with region
# - `gold.capacity_metrics_by_timepoint` — per-timepoint CU metrics (standard)
# - `gold.timepoint_metrics` — per-activity detail (standard)
# 
# ## Outputs
# None — results are displayed interactively.
# 
# ## Execution Order
# Run **after** nb_0300_generate_gold_tables. No side effects.
# 
# ## Usage
# 1. Set `start_date` and `end_date` in the Parameters cell.
# 2. Optionally filter by `target_capacity_id`.
# 3. Run all cells to see capacity utilisation, activity breakdown, and cost estimates.

# CELL ********************

from pyspark.sql.functions import col, to_timestamp, to_date
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('nb_0240')

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

# PARAMETERS CELL ********************

# ---------------------------------------------------------------------------
# Diagnostic parameters -- adjust as needed for your analysis
# ---------------------------------------------------------------------------
start_date = '04 June 2026 00:00'
end_date   = '05 June 2026 00:00'
filter_capacity_id = TARGET_CAPACITY_ID  # set to None for all capacities
pricing_type = 'PAYG'  # 'PAYG' or 'Reservation'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------
# Regional pricing reference table (PAYG USD, per CU-hour)
# Update rates here to reflect current Azure pricing for your regions
# -----------------------------------------------------------------------
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
logger.info('Pricing reference loaded: %d regions (%s)', pricing_df.count(), pricing_type)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------
# Load capacity regions — handle 'Default' region mapping
# -----------------------------------------------------------------------
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
logger.info('Capacities with pricing: %d', capacity_with_pricing.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------
# Summary: timepoint coverage check for requested date range
# -----------------------------------------------------------------------
coverage_query = f"""
    SELECT to_date(TimePoint) AS TimePointDay, COUNT(*) AS TimepointCount
    FROM {GOLD_METRICS_BY_TIMEPOINT}
    WHERE TimePoint BETWEEN to_timestamp('{start_date}', 'dd MMMM yyyy HH:mm')
                       AND to_timestamp('{end_date}',   'dd MMMM yyyy HH:mm')
"""
if filter_capacity_id:
    coverage_query += f" AND CapacityId = '{filter_capacity_id.upper()}'"
coverage_query += ' GROUP BY to_date(TimePoint) ORDER BY 1'
coverage_df = spark.sql(coverage_query)
logger.info('Timepoint coverage for range: %d day(s)', coverage_df.count())
display(coverage_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------
# Activity detail with cost estimate
# -----------------------------------------------------------------------
act_query = f"""
    SELECT
        Workspace_name, Item_name, Unique_key, Operation,
        Operation_Id, Operation_start_time, Operation_end_time,
        Status, User, Item_kind, Billing_type,
        Timepoint, CapacityId,
        SumDuration_s, SumTotal_CU_s
    FROM {GOLD_TP_METRICS}
    WHERE Timepoint BETWEEN to_timestamp('{start_date}', 'dd MMMM yyyy HH:mm')
                       AND to_timestamp('{end_date}',   'dd MMMM yyyy HH:mm')
"""
if filter_capacity_id:
    act_query += f" AND CapacityId = '{filter_capacity_id.upper()}'"
activities = spark.sql(act_query)
activities_priced = activities.join(capacity_with_pricing.select('CapacityId','Region','CU_price_per_hour'), 'CapacityId', 'left')
cost_df = activities_priced.withColumn(
    'EstimatedCost_USD',
    (col('SumTotal_CU_s') / 3600.0) * col('CU_price_per_hour')
)
logger.info('Activities loaded: %d rows', cost_df.count())
display(cost_df.select('Workspace_name','Item_name','Operation','Status','Timepoint','SumTotal_CU_s','EstimatedCost_USD')
         .orderBy('EstimatedCost_USD', ascending=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------
# Cost summary by workspace / item
# -----------------------------------------------------------------------
summary_df = cost_df.groupBy('Workspace_name','Item_name','Operation','Billing_type').agg(
    {'SumTotal_CU_s': 'sum', 'EstimatedCost_USD': 'sum', 'Operation_Id': 'count'}
).withColumnRenamed('sum(SumTotal_CU_s)', 'TotalCU_s'
).withColumnRenamed('sum(EstimatedCost_USD)', 'TotalCost_USD'
).withColumnRenamed('count(Operation_Id)', 'OperationCount')
display(summary_df.orderBy('TotalCost_USD', ascending=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }


