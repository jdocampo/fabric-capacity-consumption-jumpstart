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

# # nb_0240_collect_autoscale_timepoint_detail
# 
# ## Purpose
# Collect per-timepoint **Autoscale + Spark** operation detail from the Fabric Capacity Metrics
# semantic model. Each timepoint result is first staged into a temporary persisted
# **non-Spark** structure under `Files/tmp`, then the notebook performs one final MERGE
# into `silver.timepoint_metrics_autoscale`.
# 
# ## Inputs
# - `nb_0110_shared_config` — all config
# - `gold.capacity_calendar_timepoints` — prerequisite: must exist (run nb_0210_collect_capacity_metrics first)
# 
# ## Outputs
# - `silver.timepoint_metrics_autoscale` — autoscale operation detail, MERGED once per notebook run on CapacityId + TimePoint + Unique_key + Operation + Operation_Id + Operation_start_time + Operation_end_time
# 
# ## Execution Order
# 1. `nb_0210_collect_capacity_metrics` (populates calendar timepoints)
# 2. This notebook (independent of 0220 — can run separately or together)
# 3. `nb_0300_generate_gold_tables` (generates gold tables including cumulative CU)
# 
# ## Bug Fixes vs. original nb_0240_collect_autoscale_timepoint_detail
# - `get_detailed_data`: now `return`s immediately when `row_count == 0` (was missing, caused downstream errors)
# - `spark.catalog.tableExists()` replaces deprecated `spark._jsparkSession.catalog().tableExists()`
# - Per-timepoint silver writes replaced with temp persisted staging plus one final MERGE
# - Prerequisite check added (fail-fast instead of silent no-op)
# 
# ## NULL Unique_key Policy
# NULL-key rows (DAX grand-total rows) are stored in silver for audit but EXCLUDED from gold.
# This is intentional and documented in nb_0300_generate_gold_tables.


# CELL ********************

import sempy.fabric as fabric
from datetime import datetime
import datetime as dt
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.functions import lit, current_timestamp
from delta.tables import DeltaTable
import time
import logging
import concurrent.futures
import threading

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('nb_0230')

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

# Pipeline-injectable overrides (tag this cell as 'parameters')
metric_workspace   = globals().get('metric_workspace', METRIC_WORKSPACE_ID)
metric_dataset     = globals().get('metric_dataset', METRIC_DATASET_ID)
target_capacity_id = globals().get('target_capacity_id', TARGET_CAPACITY_ID)
start_date_str = globals().get('start_date_str', DEFAULT_START_DATE_STR)
end_date_str   = globals().get('end_date_str', DEFAULT_END_DATE_STR)

# Parallelization settings
parallel_threads = globals().get('parallel_threads', 10)  # number of parallel threads for timepoint collection

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logger.info(f"Parameters - workspace: {metric_workspace}, dataset: {metric_dataset}, capacity_id: {target_capacity_id}, start_date: {start_date_str}, end_date: {end_date_str}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

_raw_display = globals().get('display_data', DISPLAY_DATA)
display_data = _raw_display.strip().lower() in ('1', 'true', 'yes', 'y') if isinstance(_raw_display, str) else bool(_raw_display)
timepoint_calendar_table = local_table_name(SILVER_TIMEPOINTS)
silver_table_name        = local_table_name(SILVER_TP_METRICS_AS)
count_of_connectivity_errors = 0
error_counter_lock = threading.Lock()
merge_columns = ['CapacityId', 'TimePoint', 'Unique_key', 'Operation', 'Operation_Id', 'Operation_start_time', 'Operation_end_time']
temp_store_state = create_temp_persisted_store('nb_0230')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PREREQUISITE CHECK: abort if calendar timepoints are missing for the requested range
prereq_query = f"""
    SELECT COUNT(*) as n FROM {timepoint_calendar_table}
    WHERE TimePoint BETWEEN to_timestamp('{start_date_str}', 'dd MMMM yyyy HH:mm')
                       AND to_timestamp('{end_date_str}', 'dd MMMM yyyy HH:mm')
    AND CapacityId = '{target_capacity_id}'"""
try:
    prereq_count = spark.sql(prereq_query).collect()[0]['n']
except Exception as ex:
    raise RuntimeError(f'PREREQUISITE FAILED: {timepoint_calendar_table} not accessible. Run nb_0110_shared_config (or run 0200 followed by 0300) first. {ex}')
if prereq_count == 0:
    raise RuntimeError(f'PREREQUISITE FAILED: No timepoints for {target_capacity_id} in {start_date_str}--{end_date_str}. Run nb_0110_shared_config (or run 0200 followed by 0300) first.')
logger.info('Prerequisite OK: %d calendar timepoints found', prereq_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_detailed_data(capacity_id, timepoint):

    capacity_id = capacity_id.upper()
    date_obj = timepoint
    date_part = f"DATE({date_obj.year}, {date_obj.month}, {date_obj.day})"
    time_part = f"TIME({date_obj.hour}, {date_obj.minute}, {date_obj.second})"
    timepoint_expr = f"{date_part} + {time_part}"

    dax_query = f"""
    DEFINE
        MPARAMETER 'TimePoint3' = 
            ({timepoint_expr})

        MPARAMETER 'CapacitiesList' = 
            {{"{capacity_id}"}}

        VAR __DS0FilterTable = 
            TREATAS(
                {{"'TimepointDetailForWorkloadAutoscale'[Billing type]",
                    "'Timepoint Detail For Workload Autoscale'[Operation Id]",
                    "'Timepoint Detail For Workload Autoscale'[Billing type]"}},
                'Feature autoscale timepoint page optional columns'[DynamicColumnsFeatureAutoScaleTimepointOperations Fields]
            )

        VAR __DS0FilterTable2 = 
            TREATAS({{"{capacity_id}"}}, 'Capacities'[Capacity Id])

        VAR __DS0FilterTable3 = 
            TREATAS(
                {{{timepoint_expr}}},
                'Timepoints'[Time-point]
            )

        VAR __DS0Core = 
            SUMMARIZECOLUMNS(
                ROLLUPADDISSUBTOTAL(
                    ROLLUPGROUP(
                        'Timepoint Detail For Workload Autoscale'[Operation],
                        'Timepoint Detail For Workload Autoscale'[Operation start time],
                        'Timepoint Detail For Workload Autoscale'[Operation end time],
                        'Timepoint Detail For Workload Autoscale'[Status],
                        'Timepoint Detail For Workload Autoscale'[User],
                        'Items'[Workspace name],
                        'Items'[Item kind],
                        'Items'[Unique key],
                        'Timepoint Detail For Workload Autoscale'[Operation Id],
                        'Timepoint Detail For Workload Autoscale'[Billing type],
                        'Items'[Item name]
                    ), "IsGrandTotalRowTotal"
                ),
                __DS0FilterTable,
                __DS0FilterTable2,
                __DS0FilterTable3,
                "SumTimepoint_CU__s_", CALCULATE(SUM('Timepoint Detail For Workload Autoscale'[Timepoint CU (s)])),
                "Timepoint_workload_autoscale_limit", 'All Measures'[Timepoint workload autoscale limit]
            )


    EVALUATE
        __DS0Core
    """

    #print(dax_query)
    start_time = time.time()
    timepoint_summary = fabric.evaluate_dax(
        workspace=metric_workspace,
        dataset=metric_dataset,
        dax_string=dax_query
    )
    end_time = time.time()

    if display_data:
        display(timepoint_summary)

    timepoint_summary.columns = ['Operation','Operation_start_time','Operation_end_time','Status','User','Workspace_name'
        ,'Item_kind','Unique_key','Operation_Id','Billing_type','Item_name','IsGrandTotalRowTotal'
        ,'SumCU_s','Timepoint_workload_autoscale_limit']

    row_count = len(timepoint_summary.index)

    if row_count == 0:
        logger.info(
            'No autoscale rows returned for capacity=%s timepoint=%s: %.4f seconds, %d row(s)',
            capacity_id,
            date_obj,
            end_time - start_time,
            row_count,
        )
        return
        

    timepoint_summary['TimePoint'] = pd.Timestamp(date_obj)
    timepoint_summary['CapacityId'] = capacity_id
    staged_summary = timepoint_summary.drop_duplicates(subset=merge_columns)
    duplicate_count = row_count - len(staged_summary.index)
    staged_rows = append_pandas_to_temp_store(temp_store_state, staged_summary)
    logger.info(
        'Collected and staged autoscale rows for capacity=%s timepoint=%s: %.4f seconds, %d row(s)',
        capacity_id,
        date_obj,
        end_time - start_time,
        staged_rows,
    )
    if duplicate_count > 0:
        logger.warning(
            'Dropped %d duplicate autoscale source row(s) before staging for capacity=%s timepoint=%s',
            duplicate_count,
            capacity_id,
            date_obj,
        )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def process_timepoint_worker(cap_id, tp):
    """Worker function for parallel autoscale timepoint processing"""
    global count_of_connectivity_errors
    try:
        get_detailed_data(cap_id, tp)
    except Exception as ex:
        with error_counter_lock:
            count_of_connectivity_errors += 1
        logger.error('Error: capacity=%s tp=%s err=%s', cap_id, tp, ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logger.info('Using temporary staged store: %s', temp_store_state['spark_path'])
try:
    # We skip the 30-second timepoints since autoscale timepoints are 1 minute intervals.
    # So we filter out those ending with :30
    iter_query = f"""
        SELECT CapacityId, TimePoint FROM {timepoint_calendar_table}
        WHERE TimePoint BETWEEN to_timestamp('{start_date_str}', 'dd MMMM yyyy HH:mm')
                           AND to_timestamp('{end_date_str}', 'dd MMMM yyyy HH:mm')
        AND TimePoint NOT LIKE '%:30'
        AND CapacityId = '{target_capacity_id}'
        ORDER BY CapacityId, TimePoint DESC"""
    rows = spark.sql(iter_query).collect()
    logger.info('Processing %d autoscale timepoints in parallel...', len(rows))
    
    # Process timepoints in parallel using ThreadPoolExecutor
    max_workers = min(parallel_threads, len(rows))  # Limit to parallel_threads concurrent threads or number of rows
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = []
        for row in rows:
            cap_id = row['CapacityId']
            tp = to_python_datetime(row['TimePoint'])
            future = executor.submit(process_timepoint_worker, cap_id, tp)
            futures.append(future)
        
        # Wait for all tasks to complete
        concurrent.futures.wait(futures)
    
    if count_of_connectivity_errors > 0:
        logger.warning('Completed with %d error(s)', count_of_connectivity_errors)
    else:
        logger.info('Autoscale collection complete')

    staged_summary = load_temp_store_to_spark(temp_store_state)
    if staged_summary is None:
        logger.info('No staged autoscale rows found for final merge')
    else:
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
        staged_summary = (staged_summary
            .withColumn(
                'Operation_Id',
                F.when(F.col('Operation_Id').isNull(), synthetic_operation_id).otherwise(F.col('Operation_Id')),
            )
            .withColumn('insert_date', current_timestamp()))
        staged_row_count = staged_summary.count()
        deduped_summary = staged_summary.dropDuplicates(merge_columns)
        deduped_row_count = deduped_summary.count()
        duplicate_count = staged_row_count - deduped_row_count
        if duplicate_count > 0:
            logger.warning('Dropped %d duplicate staged autoscale row(s) before final merge', duplicate_count)
        merge_key = (
            "t.CapacityId = s.CapacityId "
            "AND t.TimePoint = s.TimePoint "
            "AND t.Unique_key <=> s.Unique_key "
            "AND t.Operation <=> s.Operation "
            "AND t.Operation_Id <=> s.Operation_Id "
            "AND t.Operation_start_time <=> s.Operation_start_time "
            "AND t.Operation_end_time <=> s.Operation_end_time"
        )
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                if table_exists(silver_table_name):
                    dt_s = DeltaTable.forName(spark, silver_table_name)
                    (dt_s.alias("t").merge(deduped_summary.alias("s"), merge_key)
                        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
                else:
                    deduped_summary.write.format("delta").saveAsTable(silver_table_name)
                logger.info('Final merge completed for %d staged autoscale row(s)', deduped_row_count)
                break
            except Exception as ex:
                logger.warning(f'Attempt {attempt} failed with error: {ex}')
            if attempt == max_retries:
                logger.error('Max retries reached. Failed to merge data into silver table.')
                raise
            else:
                time.sleep(30)  # wait before retrying
finally:
    delete_temp_persisted_store(temp_store_state)
    logger.info('Deleted temporary staged store: %s', temp_store_state['spark_path'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# One off deletion of 30 sec timepoints
if table_exists(silver_table_name):
    if spark.sql(f"SELECT COUNT(*) FROM {silver_table_name} WHERE date_format(TimePoint, 'ss') = '30'").collect()[0][0] > 0:
        logger.info('Deleting 30-second timepoints from silver table')
        spark.sql(f"DELETE FROM {silver_table_name} WHERE date_format(TimePoint, 'ss') = '30'")
        logger.info('Deleted 30-second timepoints from silver table')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    total = spark.sql(f'SELECT COUNT(*) as n FROM {silver_table_name}').collect()[0]['n']
    nulls = spark.sql(f'SELECT COUNT(*) as n FROM {silver_table_name} WHERE Unique_key IS NULL').collect()[0]['n']
    logger.info('Silver %s: %d total rows | %d NULL Unique_key rows', silver_table_name, total, nulls)
except Exception as ex:
    logger.warning('Count failed (non-fatal): %s', ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

maintenance_table = local_table_name(silver_table_name)
try:
    spark.sql(f'OPTIMIZE {maintenance_table}')
    spark.sql(f'VACUUM {maintenance_table} RETAIN 168 HOURS')
    spark.sql(f'ANALYZE TABLE {maintenance_table} COMPUTE STATISTICS')
    logger.info('Silver optimized: %s', silver_table_name)
except Exception as ex:
    logger.warning('Optimize failed (non-fatal): %s', ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }


