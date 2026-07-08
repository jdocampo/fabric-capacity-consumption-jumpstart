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

# # nb_0300_generate_gold_tables
# 
# ## Purpose
# Read persisted **silver** tables and generate all **gold** curated/business tables.
# This notebook is the single source of gold writes — run it after any collector (0200/0220/0230).
# It is fully idempotent: safe to re-run at any time.
# 
# ## Inputs (silver — written by collectors)
# | Silver Table | Collector | Content |
# |---|---|---|
# | `silver.capacities` | nb_0210_collect_capacity_metrics | Raw capacity list |
# | `silver.capacity_metrics_timepoints` | nb_0210_collect_capacity_metrics | Daily per-timepoint CU metrics |
# | `silver.timepoint_metrics` | nb_0220_collect_timepoint_detail | Per-timepoint activity detail (standard) |
# | `silver.timepoint_metrics_autoscale` | nb_0240_collect_autoscale_timepoint_detail | Per-timepoint activity detail (autoscale) |
# 
# ## Outputs (gold — curated for consumption)
# | Gold Table | Key | Notes |
# |---|---|---|
# | `gold.capacities` | N/A | Overwrite from silver (small, full-refresh) |
# | `gold.capacity_metrics_by_timepoint` | CapacityId + TimePoint | MERGE from silver |
# | `gold.capacity_calendar_timepoints` | CapacityId + TimePoint | DISTINCT timepoints MERGE |
# | `gold.timepoint_metrics` | CapacityId+TimePoint+Unique_key+Operation+Operation_Id+Operation_start_time+Operation_end_time | NULL Unique_key excluded |
# | `gold.timepoint_metrics_autoscale` | CapacityId+TimePoint+Unique_key+Operation+Operation_Id+Operation_start_time+Operation_end_time | Adds `Acc_CU_s` (cumulative CU) |
# 
# ## Cumulative CU (Autoscale)
# `Acc_CU_s` = running total CU in seconds:
# `SUM(SumCU_s) OVER (PARTITION BY CapacityId, Operation_Id ORDER BY Timepoint ROWS UNBOUNDED PRECEDING)`
# - **Point-in-time CU** = `SumCU_s` (per row)
# - **Cumulative total CU at latest timepoint** = `MAX(Acc_CU_s)` per CapacityId+Operation_Id
# 
# ## NULL Unique_key Policy
# NULL Unique_key rows are DAX grand-total rows. They are stored in silver for audit
# but intentionally excluded from gold to avoid key ambiguity and inflated aggregates.
# 
# ## Execution Order
# Run **after** one or more of: nb_0210_collect_capacity_metrics, nb_0220_collect_timepoint_detail, nb_0240_collect_autoscale_timepoint_detail.
# Optional silver tables (0220/0230 outputs) are skipped gracefully if not yet populated.


# CELL ********************

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('nb_0300')

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

# Pipeline-injectable overrides -- add here if needed
# (all targets are defined in nb_0110_shared_config)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Ensure schemas exist before any writes
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}')
logger.info('Schemas verified: %s / %s', SILVER_SCHEMA, GOLD_SCHEMA)

silver_capacities_table = local_table_name(SILVER_CAPACITIES)
silver_timepoints_table = local_table_name(SILVER_TIMEPOINTS)
silver_tp_metrics_table = local_table_name(SILVER_TP_METRICS)
silver_tp_metrics_as_table = local_table_name(SILVER_TP_METRICS_AS)
gold_capacities_table = local_table_name(GOLD_CAPACITIES)
gold_metrics_by_timepoint_table = local_table_name(GOLD_METRICS_BY_TIMEPOINT)
gold_calendar_timepoints_table = local_table_name(GOLD_CALENDAR_TIMEPOINTS)
gold_tp_metrics_table = local_table_name(GOLD_TP_METRICS)
gold_tp_metrics_as_table = local_table_name(GOLD_TP_METRICS_AS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------
# Prerequisite checks — verify required silver tables exist
# -----------------------------------------------------------------------
def dedupe_for_merge(df, keys):
    if 'insert_date' in df.columns:
        window_spec = Window.partitionBy(*keys).orderBy(F.col('insert_date').desc_nulls_last())
        return (df.withColumn('_merge_rank', F.row_number().over(window_spec))
                  .filter(F.col('_merge_rank') == 1)
                  .drop('_merge_rank'))
    return df.dropDuplicates(keys)


def check_silver(table_name, required=True):
    if not table_exists(table_name):
        if required:
            raise RuntimeError(f'PREREQUISITE FAILED: {table_name} missing. Run the appropriate collector first.')
        logger.warning('Optional silver table not found: %s — will skip', table_name)
        return False
    cnt = spark.sql(f'SELECT COUNT(*) as n FROM {local_table_name(table_name)}').collect()[0]['n']
    logger.info('Silver %s: %d rows', table_name, cnt)
    return cnt > 0


def with_synthetic_operation_id(df):
    required_columns = {'Operation_Id', 'Operation_start_time', 'Operation_end_time'}
    if not required_columns.issubset(set(df.columns)):
        return df
    synthetic_operation_id = F.concat(
        F.lit('synthetic-'),
        F.sha2(
            F.concat_ws(
                '||',
                F.coalesce(F.col('Operation_start_time').cast('string'), F.lit('NULL_START')),
                F.coalesce(F.col('Operation_end_time').cast('string'), F.lit('NULL_END')),
            ),
            256,
        ),
    )
    return df.withColumn(
        'Operation_Id',
        F.when(F.col('Operation_Id').isNull(), synthetic_operation_id).otherwise(F.col('Operation_Id')),
    )

check_silver(SILVER_CAPACITIES, required=True)
check_silver(SILVER_TIMEPOINTS, required=True)
has_tp_metrics    = check_silver(SILVER_TP_METRICS,    required=False)
has_tp_metrics_as = check_silver(SILVER_TP_METRICS_AS, required=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------
# GOLD 1: gold.capacities (overwrite — small table, full-refresh each run)
# -----------------------------------------------------------------------
logger.info('Writing gold.capacities...')
cap_df = spark.sql(f'SELECT * FROM {silver_capacities_table}')
cap_df.write.mode('overwrite').option('mergeSchema','true').format('delta').saveAsTable(gold_capacities_table)
logger.info('gold.capacities: %d rows', cap_df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------
# GOLD 2: gold.capacity_metrics_by_timepoint (MERGE on CapacityId + TimePoint)
# -----------------------------------------------------------------------
logger.info('Writing gold.capacity_metrics_by_timepoint...')
tp_df = dedupe_for_merge(
    spark.sql(f'SELECT * FROM {silver_timepoints_table}'),
    ['CapacityId', 'TimePoint'],
)
tp_df.cache()
if table_exists(GOLD_METRICS_BY_TIMEPOINT):
    gdt = DeltaTable.forName(spark, local_table_name(GOLD_METRICS_BY_TIMEPOINT))
    (gdt.alias('t').merge(tp_df.alias('s'), 's.CapacityId = t.CapacityId AND s.TimePoint = t.TimePoint')
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    logger.info('gold.capacity_metrics_by_timepoint MERGED')
else:
    tp_df.write.format('delta').saveAsTable(gold_metrics_by_timepoint_table)
    logger.info('gold.capacity_metrics_by_timepoint CREATED')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------
# GOLD 3: gold.capacity_calendar_timepoints (DISTINCT timepoints from silver)
# -----------------------------------------------------------------------
logger.info('Writing gold.capacity_calendar_timepoints...')
cal_df = spark.sql(f'SELECT DISTINCT CapacityId, TimePoint, to_date(TimePoint) AS Date FROM {silver_timepoints_table}')
if table_exists(GOLD_CALENDAR_TIMEPOINTS):
    gcal = DeltaTable.forName(spark, local_table_name(GOLD_CALENDAR_TIMEPOINTS))
    (gcal.alias('t').merge(cal_df.alias('s'), 's.CapacityId = t.CapacityId AND s.TimePoint = t.TimePoint')
        .whenNotMatchedInsertAll().execute())
    logger.info('gold.capacity_calendar_timepoints MERGED')
else:
    cal_df.write.format('delta').saveAsTable(gold_calendar_timepoints_table)
    logger.info('gold.capacity_calendar_timepoints CREATED')
tp_df.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------
# GOLD 4: gold.timepoint_metrics (NULL Unique_key rows EXCLUDED)
# -----------------------------------------------------------------------
if has_tp_metrics:
    logger.info('Writing gold.timepoint_metrics...')
    null_cnt = spark.sql(f'SELECT COUNT(*) as n FROM {silver_tp_metrics_table} WHERE Unique_key IS NULL').collect()[0]['n']
    if null_cnt > 0:
        logger.warning('Excluding %d NULL Unique_key rows (grand-total DAX rows) from gold', null_cnt)
    tm_df = dedupe_for_merge(
        with_synthetic_operation_id(spark.sql(f'SELECT * FROM {silver_tp_metrics_table} WHERE Unique_key IS NOT NULL')),
        ['CapacityId', 'TimePoint', 'Unique_key', 'Operation', 'Operation_Id', 'Operation_start_time', 'Operation_end_time'],
    )
    mk = (
        's.CapacityId=t.CapacityId '
        'AND s.TimePoint=t.TimePoint '
        'AND s.Unique_key <=> t.Unique_key '
        'AND s.Operation <=> t.Operation '
        'AND s.Operation_Id <=> t.Operation_Id '
        'AND s.Operation_start_time <=> t.Operation_start_time '
        'AND s.Operation_end_time <=> t.Operation_end_time'
    )
    if table_exists(GOLD_TP_METRICS):
        gtm = DeltaTable.forName(spark, local_table_name(GOLD_TP_METRICS))
        (gtm.alias('t').merge(tm_df.alias('s'), mk).whenNotMatchedInsertAll().execute())
        logger.info('gold.timepoint_metrics MERGED: %d rows', tm_df.count())
    else:
        tm_df.write.format('delta').saveAsTable(gold_tp_metrics_table)
        logger.info('gold.timepoint_metrics CREATED: %d rows', tm_df.count())
else:
    logger.warning('Skipping gold.timepoint_metrics — silver table empty or missing')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------
# GOLD 5: gold.timepoint_metrics_autoscale
#   Adds Acc_CU_s = cumulative running-total CU per CapacityId + Operation_Id
#   Point-in-time CU  = SumCU_s (per row, already in silver)
#   Cumulative CU at latest timepoint = MAX(Acc_CU_s) per CapacityId + Operation_Id
# -----------------------------------------------------------------------
if has_tp_metrics_as:
    logger.info('Writing gold.timepoint_metrics_autoscale with cumulative CU...')
    null_cnt_as = spark.sql(f'SELECT COUNT(*) as n FROM {silver_tp_metrics_as_table} WHERE Unique_key IS NULL').collect()[0]['n']
    if null_cnt_as > 0:
        logger.warning('Excluding %d NULL Unique_key rows from autoscale gold', null_cnt_as)
    minute_df = spark.sql(f"SELECT * FROM {silver_tp_metrics_as_table} WHERE Unique_key IS NOT NULL AND Timepoint NOT LIKE '%:30'")
    as_df = dedupe_for_merge(
        with_synthetic_operation_id(minute_df),
        ['CapacityId', 'TimePoint', 'Unique_key', 'Operation', 'Operation_Id', 'Operation_start_time', 'Operation_end_time'],
    )
    win = (Window
           .partitionBy('CapacityId', 'Unique_key','Operation_Id')
           .orderBy('Timepoint')
           .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    as_gold = as_df.withColumn('Acc_CU_s', F.sum('SumCU_s').over(win))
    mk_as = (
        's.CapacityId=t.CapacityId '
        'AND s.TimePoint=t.TimePoint '
        'AND s.Unique_key <=> t.Unique_key '
        'AND s.Operation <=> t.Operation '
        'AND s.Operation_Id <=> t.Operation_Id '
        'AND s.Operation_start_time <=> t.Operation_start_time '
        'AND s.Operation_end_time <=> t.Operation_end_time'
    )
    if table_exists(GOLD_TP_METRICS_AS):
        gas = DeltaTable.forName(spark, local_table_name(GOLD_TP_METRICS_AS))
        (gas.alias('t').merge(as_gold.alias('s'), mk_as)
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        logger.info('gold.timepoint_metrics_autoscale MERGED (with Acc_CU_s)')

        # Delete all rows which timepoint ends with the :30 second value
        gas.delete("TimePoint LIKE '%:30'")
        logger.info('Deleted rows with :30 seconds from gold.timepoint_metrics_autoscale')
    else:
        as_gold.write.format('delta').saveAsTable(gold_tp_metrics_as_table)
        logger.info('gold.timepoint_metrics_autoscale CREATED (with Acc_CU_s)')
else:
    logger.warning('Skipping gold.timepoint_metrics_autoscale — silver table empty or missing')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------
# Optimize all gold tables
# -----------------------------------------------------------------------
gold_tables = [GOLD_CAPACITIES, GOLD_METRICS_BY_TIMEPOINT, GOLD_CALENDAR_TIMEPOINTS]
if has_tp_metrics:
    gold_tables.append(GOLD_TP_METRICS)
if has_tp_metrics_as:
    gold_tables.append(GOLD_TP_METRICS_AS)
for tbl in gold_tables:
    try:
        spark.sql(f'OPTIMIZE {local_table_name(tbl)}')
        logger.info('Optimized: %s', tbl)
    except Exception as ex:
        logger.warning('OPTIMIZE skipped for %s: %s', tbl, ex)
logger.info('nb_0300_generate_gold_tables COMPLETE — all gold tables up to date')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }


