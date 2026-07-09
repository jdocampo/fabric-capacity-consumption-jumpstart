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

# # collect_capacity_metrics
# 
# ## Purpose
# Collect daily capacity-level CU metrics (timepoints) from the **Fabric Capacity Metrics** semantic model
# and persist raw data into the **silver** layer for downstream gold generation.
# 
# ## Inputs
# - `shared_config` (loaded via `%run`) — all workspace/model/table config
# - `silver.capacity_calendar_timepoints` is written here as a side-output (DISTINCT timepoints found)
# 
# ## Outputs
# - `silver.capacities` — raw capacity list from the semantic model (overwrite on each run)
# - `silver.capacity_metrics_timepoints` — per-timepoint CU metrics, appended (idempotent via MERGE on CapacityId+TimePoint)
# 
# ## Execution Order
# Run **before** `generate_gold_tables`.
# Run **independently** of `collect_timepoint_detail` / `collect_autoscale_timepoint_detail` (no data dependency between them).
# 
# ## Parameters (override after %run if needed)
# | Param | Default | Description |
# |---|---|---|
# | DEFAULT_START_DATE_STR | from config | Shared collection window start (`dd MMMM yyyy HH:mm`) |
# | DEFAULT_END_DATE_STR | from config | Shared collection window end (`dd MMMM yyyy HH:mm`) |
# | TARGET_CAPACITY_ID | from config | Set to `None` to collect ALL capacities |
# | DISPLAY_DATA | False | Set True for interactive debugging |


# CELL ********************

import sempy.fabric as fabric
from datetime import datetime, timedelta
import datetime as dt
import pyspark.sql.functions as F
from pyspark.sql.functions import lit, current_timestamp
from delta.tables import DeltaTable
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('collect_capacity_metrics')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run "./shared_config"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# ---------------------------------------------------------------------------
# Pipeline-injectable overrides -- tag this cell as 'parameters' for ADF/Synapse
# Defaults come from shared_config; override here or via parent orchestration.
# ---------------------------------------------------------------------------
metric_workspace = globals().get('metric_workspace', METRIC_WORKSPACE_ID)
metric_dataset   = globals().get('metric_dataset', METRIC_DATASET_ID)
target_capacity_id = globals().get('target_capacity_id', TARGET_CAPACITY_ID)  # set to None to collect ALL capacities
start_date_str = globals().get('start_date_str', DEFAULT_START_DATE_STR)
end_date_str   = globals().get('end_date_str', DEFAULT_END_DATE_STR)
_raw_display = globals().get('display_data', DISPLAY_DATA)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logger.info(f"Parameters - workspace: {metric_workspace}, dataset: {metric_dataset}, capacity_id: {target_capacity_id}, start_date: {start_date_str}, end_date: {end_date_str}")

# CELL ********************

display_data = _raw_display.strip().lower() in ('1', 'true', 'yes', 'y') if isinstance(_raw_display, str) else bool(_raw_display)

# Table targets (resolved from config -- do not redefine these manually)
silver_capacities = local_table_name(SILVER_CAPACITIES)
silver_table_name = local_table_name(SILVER_TIMEPOINTS)
test_mode = display_data
count_of_connectivity_errors = 0
window_format = '%d %B %Y %H:%M'
start_window = datetime.strptime(start_date_str, window_format)
end_window = datetime.strptime(end_date_str, window_format)
if end_window < start_window:
    raise ValueError(f'end_date_str {end_date_str} must be on or after start_date_str {start_date_str}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Ensure silver and gold schemas exist in the attached lakehouse catalog
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}')
logger.info('Schemas verified: silver=%s, gold=%s', SILVER_SCHEMA, GOLD_SCHEMA)
logger.info(
    'Collection window aligned to shared defaults: %s -> %s',
    start_window.strftime(window_format),
    end_window.strftime(window_format),
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check Table Status
version = ''

try: 
    check_table_structure_query = """EVALUATE ROW("Blocked_workspaces__Day_", 'All Measures'[Blocked workspaces (Day)])"""
    check_table_structure_df = fabric.evaluate_dax(workspace=metric_workspace, dataset=metric_dataset, dax_string=check_table_structure_query)
    print("INFO: Test for v53 successful")
    version = 'v53'
except:
    print("INFO: Test for v53 failed")

if version == '':
    try: 
        check_table_structure_query = """DEFINE    MPARAMETER 'DefaultCapacityID' = "0000000-0000-0000-0000-00000000"
                                        EVALUATE   SUMMARIZECOLUMNS("Background billable CU %", [Background billable CU %]    )"""
        check_table_structure_df = fabric.evaluate_dax(workspace=metric_workspace, dataset=metric_dataset, dax_string=check_table_structure_query)
        print("INFO: Test for v47 successful")
        version = 'v47'
    except:
        print("INFO: Test for v47 failed")


if version == '':
    try:
        check_table_structure_query = """EVALUATE ROW("Background billable CU %", 'All Measures'[Background billable CU %])"""
        check_table_structure_df = fabric.evaluate_dax(workspace=metric_workspace, dataset=metric_dataset, dax_string=check_table_structure_query)
        print("INFO: Test for v40 successful")
        version = 'v40'
    except:
        print("INFO: Test for v40 failed")

if version == '':
    try:
        check_table_structure_query_alternative = """EVALUATE ROW("xBackground__", 'All Measures'[xBackground %])"""
        check_table_structure_df_alternative = fabric.evaluate_dax(workspace=metric_workspace, dataset=metric_dataset, dax_string=check_table_structure_query_alternative)
        version = 'v37'
        print("INFO: Test for v37 successful")
    except:
        print("INFO: Test for v37 failed")


# Validate version compatibility
if version != '':
    print( f'INFO: Version {version} is valid')
else: 
    raise Exception("ERROR: Capacity Metrics data structure is not compatible or connection to capacity metrics is not possible.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fetch capacities from connected capacity metrics app
try:
  if version in ['v53']:
    capacity_query = """EVALUATE SELECTCOLUMNS (    Capacities, "Capacity Id", Capacities[Capacity Id] ,"Capacity Name", Capacities[Capacity name] , "state" , Capacities[state], "Region" , Capacities[Region]  )"""
  elif version in ['v47', 'v44', 'v40']:
    capacity_query = """EVALUATE SELECTCOLUMNS (    Capacities, "capacity Id", Capacities[capacity Id] , "state" , Capacities[state] , "Region" , Capacities[Region] )"""
  else:
    capacity_query = """EVALUATE SELECTCOLUMNS (    Capacities, "capacity Id", Capacities[CapacityId] , "state" , Capacities[state], "Region" , Capacities[Region]  )"""
  capacities = fabric.evaluate_dax(workspace=metric_workspace, dataset=metric_dataset, dax_string=capacity_query)
  capacities.columns = ['CapacityId', 'CapacityName', 'State', 'Region']
  capacities = spark.createDataFrame(capacities)
except Exception as e:
  notebookutils.notebook.exit("ERROR: Capacity Metrics data structure is not compatible or connection to capacity metrics is not possible.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write capacities to silver
cap_spark_df = capacities
capacity_count = cap_spark_df.count()
logger.info('Writing %d capacities to %s', capacity_count, silver_capacities)
# Merge the changes to the silver_capacities ignoring already existing capacities by id
from delta.tables import DeltaTable
if spark.catalog.tableExists(silver_capacities):
    logger.info('Merging new capacities into existing silver table')
    delta_table = DeltaTable.forName(spark, silver_capacities)
    delta_table.alias("target").merge(
        cap_spark_df.alias("source"),
        "target.CapacityId = source.CapacityId"
    ).whenNotMatchedInsertAll().execute()
else:
    logger.info('Creating new silver table for capacities')
    cap_spark_df.write.mode('overwrite').option('mergeSchema','true').format('delta').saveAsTable(silver_capacities)
logger.info('Capacities written to silver: %s', silver_capacities)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def iterate_dates(start_date, end_date=None):
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date or start_date, '%Y-%m-%d')
    if end_dt < start_dt:
        raise ValueError(f'end_date {end_date} must be on or after start_date {start_date}')

    dates = []
    current_dt = start_dt
    while current_dt <= end_dt:
        dates.append({
            'year': current_dt.year,
            'month': current_dt.month,
            'day': current_dt.day,
        })
        current_dt += timedelta(days=1)
    return dates

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Iterate capacities and days
capacities_to_process = capacities
if target_capacity_id:
    capacities_to_process = capacities.filter(F.upper(F.col('CapacityId')) == target_capacity_id.upper())
    logger.info('Filtered capacities to target capacity %s', target_capacity_id)

for cap in capacities_to_process.collect():
    capacity_id = cap['CapacityId']

    logger.info('Scoped CapacityId: %s', capacity_id)

    try:
        start_day_str = start_window.strftime('%Y-%m-%d')
        end_day_str = end_window.strftime('%Y-%m-%d')
        date_array = iterate_dates(start_day_str, end_date=end_day_str)
        logger.info(
            'Get data for CapacityId: %s within aligned window %s -> %s',
            capacity_id,
            start_date_str,
            end_date_str,
        )

        # Iterate days for current capacity
        for date in date_array:
            year = date['year']
            month = date['month']
            day = date['day']
            date_label = str(year) + '-' + str(month) + '-' + str(day)
            if test_mode:
                print(f"Getting data for {date_label}")

            dax_query_v53 = f"""
            DEFINE
            
             MPARAMETER 'CapacitiesList' = {{ \"{capacity_id}\" }}

            VAR __DS0Core = 
                    SUMMARIZECOLUMNS(
                        Capacities[capacity Id],
                        'TimePoints'[TimePoint],
                        TREATAS({{"{capacity_id}"}}, 'Capacities'[Capacity Id]),
                        TREATAS({{DATE({year}, {month}, {day})}}, 'Dates'[Date]),
                        "B_P", 'All Measures'[Background billable CU %],
                        "I_P", 'All Measures'[Interactive billable CU %],
                        "B_NB_P", 'All Measures'[Background non billable CU %],
                        "I_NB_P", 'All Measures'[Interactive non billable CU %],
                        "AS_P", 'All Measures'[SKU CU by timepoint %],
                        "CU_L", 'All Measures'[CU limit],
                        "T_CU_U_P", 'All Measures'[Cumulative CU usage % preview],
                        "C_CU_U_S", 'All Measures'[Cumulative CU usage (s)],
                        "SKU_CU_TP", 'All Measures'[SKU CU by timepoint],
                        "I_Del_P", 'All Measures'[Dynamic interactive delay %],
                        "I_Rej_P", 'All Measures'[Dynamic interactive rejection %],
                        "I_Rej_TH", 'All Measures'[Interactive rejection threshold],
                        "B_Rej_P", 'All Measures'[Dynamic background rejection %],
                        "B_Rej_TH", 'All Measures'[Background rejection threshold],
                        "CO_A_P", 'All Measures'[Carry over add %],
                        "CO_BD_P", 'All Measures'[Carry over burndown %],
                        "CO_C_P", 'All Measures'[Cumulative carry over %],
                        "OV_RL", 'All Measures'[Overage reference line],
                        "Exp_BD_M", 'All Measures'[Expected burndown in minutes]
                    )

            EVALUATE
                __DS0Core
            """

            dax_query_v47 = f"""
            DEFINE
            
             MPARAMETER 'CapacitiesList' = {{ \"{capacity_id}\" }}

            VAR __DS0Core = 
                    SUMMARIZECOLUMNS(
                        Capacities[capacity Id],
                        'TimePoints'[TimePoint],
                        FILTER(Capacities, Capacities[capacity Id] = \"{capacity_id}\" ),
                        FILTER(TimePoints,  'TimePoints'[Date] = DATE({year}, {month}, {day})),
                        "B_P", 'All Measures'[Background billable CU %],
                        "I_P", 'All Measures'[Interactive billable CU %],
                        "B_NB_P", 'All Measures'[Background non billable CU %],
                        "I_NB_P", 'All Measures'[Interactive non billable CU %],
                        "AS_P", 'All Measures'[SKU CU by timepoint %],
                        "CU_L", 'All Measures'[CU limit],
                        "T_CU_U_P", 'All Measures'[Cumulative CU usage % preview],
                        "C_CU_U_S", 'All Measures'[Cumulative CU usage (s)],
                        "SKU_CU_TP", 'All Measures'[SKU CU by timepoint],
                        "I_Del_P", 'All Measures'[Dynamic interactive delay %],
                        "I_Rej_P", 'All Measures'[Dynamic interactive rejection %],
                        "I_Rej_TH", 'All Measures'[Interactive rejection threshold],
                        "B_Rej_P", 'All Measures'[Dynamic background rejection %],
                        "B_Rej_TH", 'All Measures'[Background rejection threshold],
                        "CO_A_P", 'All Measures'[Carry over add %],
                        "CO_BD_P", 'All Measures'[Carry over burndown %],
                        "CO_C_P", 'All Measures'[Cumulative carry over %],
                        "OV_RL", 'All Measures'[Overage reference line],
                        "Exp_BD_M", 'All Measures'[Expected burndown in minutes]
                    )

            EVALUATE
                __DS0Core
            """
            
            dax_query_v44 = f"""
            DEFINE
            
             MPARAMETER 'CapacitiesList' = {{ \"{capacity_id}\" }}

            VAR __DS0Core = 
                    SUMMARIZECOLUMNS(
                        Capacities[capacity Id],
                        'TimePoints'[TimePoint],
                        FILTER(Capacities, Capacities[capacity Id] = \"{capacity_id}\" ),
                        FILTER(TimePoints,  'TimePoints'[Date] = DATE({year}, {month}, {day})),
                        "B_P", 'All Measures'[Background billable CU %],
                        "I_P", 'All Measures'[Interactive billable CU %],
                        "B_NB_P", 'All Measures'[Background non billable CU %],
                        "I_NB_P", 'All Measures'[Interactive non billable CU %],
                        "AS_P", 'All Measures'[SKU CU by timepoint %],
                        "CU_L", 'All Measures'[CU limit],
                        "T_CU_U_P", 'All Measures'[Cumulative CU usage % preview],
                        "C_CU_U_S", 'All Measures'[Cumulative CU usage (s)],
                        "SKU_CU_TP", 'All Measures'[SKU CU by timepoint],
                        "I_Del_P", 'All Measures'[Dynamic interactive delay %],
                        "I_Rej_P", 'All Measures'[Dynamic interactive rejection %],
                        "I_Rej_TH", 'All Measures'[Interactive rejection threshold],
                        "B_Rej_P", 'All Measures'[Dynamic background rejection %],
                        "B_Rej_TH", 'All Measures'[Background rejection threshold],
                        "CO_A_P", 'All Measures'[Carry over add %],
                        "CO_BD_P", 'All Measures'[Carry over burndown %],
                        "CO_C_P", 'All Measures'[Cumulative carry over %],
                        "OV_RL", 'All Measures'[Overage reference line],
                        "Exp_BD_M", 'All Measures'[Expected burndown in minutes]
                    )

            EVALUATE
                __DS0Core
            """


            dax_query_v40 = f"""
            DEFINE
            
            MPARAMETER 'CapacityID' = \"{capacity_id}\"

            VAR __DS0Core = 
                    SUMMARIZECOLUMNS(
                        Capacities[capacity Id],
                        'TimePoints'[TimePoint],
                        FILTER(Capacities, Capacities[capacity Id] = \"{capacity_id}\" ),
                        FILTER(TimePoints,  'TimePoints'[Date] = DATE({year}, {month}, {day})),
                        "B_P", 'All Measures'[Background billable CU %],
                        "I_P", 'All Measures'[Interactive billable CU %],
                        "B_NB_P", 'All Measures'[Background non billable CU %],
                        "I_NB_P", 'All Measures'[Interactive non billable CU %],
                        "AS_P", 'All Measures'[SKU CU by timepoint %],
                        "CU_L", 'All Measures'[CU limit],
                        "T_CU_U_P", 'All Measures'[Cumulative CU usage % preview],
                        "C_CU_U_S", 'All Measures'[Cumulative CU usage (s)],
                        "SKU_CU_TP", 'All Measures'[SKU CU by timepoint],
                        "I_Del_P", 'All Measures'[Dynamic interactive delay %],
                        "I_Rej_P", 'All Measures'[Dynamic interactive rejection %],
                        "I_Rej_TH", 'All Measures'[Interactive rejection threshold],
                        "B_Rej_P", 'All Measures'[Dynamic background rejection %],
                        "B_Rej_TH", 'All Measures'[Background rejection threshold],
                        "CO_A_P", 'All Measures'[Carry over add %],
                        "CO_BD_P", 'All Measures'[Carry over burndown %],
                        "CO_C_P", 'All Measures'[Cumulative carry over %],
                        "OV_RL", 'All Measures'[Overage reference line],
                        "Exp_BD_M", 'All Measures'[Expected burndown in minutes]
                    )

            EVALUATE
                __DS0Core
            """

            dax_query_v37 = f"""
            DEFINE

            MPARAMETER 'CapacityID' = \"{capacity_id}\"

            VAR __DS0Core = 
                    SUMMARIZECOLUMNS(
                        Capacities[capacityId],
                        'TimePoints'[TimePoint],
                        FILTER(Capacities, Capacities[capacityId] = \"{capacity_id}\" ),
                        FILTER(TimePoints,  'TimePoints'[Date] = DATE({year}, {month}, {day})),
                        "B_P", 'All Measures'[xBackground %],
                        "I_P", 'All Measures'[xInteractive %],
                        "B_NB_P", 'All Measures'[xBackground % Preview],
                        "I_NB_P", 'All Measures'[xInteractive % Preview],
                        "AS_P", 'All Measures'[SKU CU by TimePoint %],
                        "CU_L", 'All Measures'[CU Limit],
                        "T_CU_U_P", 'All Measures'[Cumulative CU Usage % Preview],
                        "C_CU_U_S", 'All Measures'[Cumulative CU Usage (s)],
                        "SKU_CU_TP", 'All Measures'[SKU CU by TimePoint],
                        "I_Del_P", 'All Measures'[Dynamic InteractiveDelay %],
                        "I_Rej_P", 'All Measures'[Dynamic InteractiveRejection %],
                        "I_Rej_TH", 'All Measures'[Interactive rejection threshold],
                        "B_Rej_P", 'All Measures'[Dynamic BackgroundRejection %],
                        "B_Rej_TH", 'All Measures'[Background rejection threshold],
                        "CO_A_P", 'All Measures'[xCarryOver_added %],
                        "CO_BD_P", 'All Measures'[xCarryOver_burndown %],
                        "CO_C_P", 'All Measures'[xCarryOver_Cumulative %],
                        "OV_RL", 'All Measures'[Overage reference line],
                        "Exp_BD_M", 'All Measures'[Expected burndown in minutes]
                    )
            EVALUATE
                __DS0Core
            """

            dax_query = ""
            # Choose query
            if version == 'v53':
                print("INFO: v53 selected")
                dax_query = dax_query_v53
            elif version == 'v47':
                print("INFO: v47 selected")
                dax_query = dax_query_v47
            elif version == 'v44':
                print("INFO: v44 selected")
                dax_query = dax_query_v44
            elif version == 'v40':
                print("INFO: v40 selected")
                dax_query = dax_query_v40
            elif version == 'v37':
                print("INFO: v37 selected")
                dax_query = dax_query_v37
                
            # Execute DAX query
            capacity_df = fabric.evaluate_dax(workspace=metric_workspace, dataset=metric_dataset, dax_string=dax_query)
            capacity_df.columns = ['CapacityId', 'TimePoint', 'BackgroundPercentage', 'InteractivePercentage', 
                                    'BackgroundNonBillablePercentage', 'InteractiveNonBillablePercentage', 'AutoscalePercentage', 
                                    'CULimitPercentage', 'TotalCUUsagePercentage', 'TotalCUs', 'SKUCUByTimePoint', 
                                    'InteractiveDelayPercentage', 'InteractiveRejectionPercentage', 'InteractiveRejectionThreshold', 
                                    'BackgroundRejectionPercentage', 'BackgroundRejectionThreshold', 'CarryOverAddedPercentage', 
                                    'CarryOverBurndownPercentage', 'CarryOverCumulativePercentage', 'OverageReferenceLine', 
                                    'ExpectedBurndownInMin']
            
            if not(capacity_df.empty):
                # Transfer pandas df to spark df
                capacity_df = spark.createDataFrame(capacity_df)

                if display_data:
                    display(capacity_df)

                # Write prepared bronze_df to silver delta table
                row_count = capacity_df.count()
                logger.info('Appending data for CapacityId: %s on Date: %s : %d row(s)', capacity_id, date_label, row_count)
                # only insert new rows, do not generate duplicates for existing rows with same CapacityId and TimePoint
                capacity_df = capacity_df.dropDuplicates(['CapacityId', 'TimePoint'])
                # Perform the merge operation to avoid duplicates based on CapacityId and TimePoint
                max_retries = 3
                for attempt in range(1, max_retries + 1):
                    try:
                        if spark.catalog.tableExists(silver_table_name):
                            delta_table = DeltaTable.forName(spark, silver_table_name)
                            delta_table.alias("target").merge(
                                capacity_df.alias("source"),
                                "target.CapacityId = source.CapacityId AND target.TimePoint = source.TimePoint"
                            ).whenNotMatchedInsertAll().execute()
                        else:
                            capacity_df.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable(silver_table_name)  
                        break
                    except Exception as ex:
                        logger.warning(f'Attempt {attempt} failed with error: {ex}')
                    if attempt == max_retries:
                        logger.error('Max retries reached. Failed to merge data into silver table.')
                        raise
                    else:
                        time.sleep(5)  # wait before retrying
            else:
                logger.info('No data for CapacityId: %s on Date: %s', capacity_id, date_label)

    except Exception as ex:
        count_of_connectivity_errors += 1
        logger.error('Collection error for CapacityId %s: %s', capacity_id, ex)
        continue

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if count_of_connectivity_errors > 0:
    logger.warning('Completed with %d connectivity error(s). Check logs above.', count_of_connectivity_errors)
else:
    logger.info('Collection complete — no connectivity errors')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Log silver table stats
try:
    silver_count = spark.sql(f'SELECT COUNT(*) as n FROM {silver_table_name}').collect()[0]['n']
    logger.info('Silver table %s: %d rows total', silver_table_name, silver_count)
except Exception as ex:
    logger.warning('Could not count silver rows: %s', ex)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Optimize silver tables for downstream reads
logger.info('Optimizing silver tables...')

for table_name in [silver_table_name, silver_capacities]:
    try:
        spark.sql(f"OPTIMIZE {local_table_name(table_name)}")
        logger.info('Optimized: %s', table_name)
    except Exception as ex:
        logger.warning('OPTIMIZE skipped for %s: %s', table_name, ex)
logger.info('Silver optimization complete')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }





