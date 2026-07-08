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

# # nb_0200_admin_orchestrator
# 
# ## Purpose
# This notebook is now the **operating guide** for the Capacity Metrics solution.
# It no longer executes downstream notebooks.
# 
# `notebookutils.notebook.runMultiple` was removed from this notebook because it is not a reliable execution mechanism for this solution.
# Use **Fabric Data Pipelines** or run the notebooks manually in the documented order below.
# 
# ## Managed assets
# - `nb_0110_shared_config`
# - `nb_0210_collect_capacity_metrics`
# - `nb_0220_collect_timepoint_detail`
# - `nb_0240_collect_autoscale_timepoint_detail`
# - `nb_0300_generate_gold_tables`
# - `nb_0400_analyze_cu_consumption_patterns`
# 
# ## Operating model
# - One lakehouse: `capacity_metrics`
# - Schemas: `silver` and `gold`
# - Single-node Spark cluster
# - Cross-workspace semantic model support via shared config
# - Shared non-Spark temp staging under `Files/tmp/capacity_metrics`
# 
# ## What to run
# | Mode | Preferred entry point | Notebook flow |
# |---|---|---|
# | `capacity` / `normal` | `pl_0100_orchestrate_capacity_consumption` | `0200 -> 0300(prep) -> 0220 -> 0300(final)` |
# | `autoscale` | `pl_0100_orchestrate_capacity_consumption` | `0200 -> 0300(prep) -> 0230 -> 0300(final)` |
# | `both` | `pl_0100_orchestrate_capacity_consumption` | `0200 -> 0300(prep) -> 0220 + 0230 -> 0300(final)` |
# 
# The first `0300` run materializes `gold.capacity_calendar_timepoints`, which both detail collectors require.
# `0220` and `0230` have no direct dependency on each other and may run in parallel only after the prep `0300` succeeds.
# 
# ## How to build or update the Data Pipelines
# 1. Do **not** call this notebook from a pipeline.
# 2. Use direct notebook activities that target `0200`, `0220`, `0230`, and `0300`.
# 3. Always start with `0200`, then run `0300` once to prepare `gold.capacity_calendar_timepoints`.
# 4. For `capacity`, run `0220` after the prep `0300`, then run `0300` again.
# 5. For `autoscale`, run `0230` after the prep `0300`, then run `0300` again.
# 6. For `both`, run `0220` and `0230` in parallel after the prep `0300`, then run the final `0300` only after both succeed.
# 7. Keep every pipeline activity name at **60 characters or fewer**.
# 8. Keep notebook defaults unless you intentionally override `start_date_str`, `end_date_str`, `target_capacity_id`, or `display_data`.
# 
# ## Recommended activity names
# - `Run_0200_CollectCapacityMetrics`
# - `Run_0300_GenerateGoldTables_CalendarPrep`
# - `Run_0220_CollectCapacityMetricsTimepointDetail`
# - `Run_0230_CollectAutoscaleDetail`
# - `Run_0300_GenerateGoldTables_Final`
# 
# ## Timeout guidance
# - `0200` and `0300`: 3600 seconds per cell
# - `0220` and `0230`: 14400 seconds per cell
# 
# The detail collectors can legitimately run for well over one hour on the single-node cluster.
# 
# ## Shared configuration procedure
# Update only `nb_0110_shared_config` for durable configuration changes.
# 
# ### Required settings
# 1. `METRIC_WORKSPACE_ID` - workspace that hosts the Fabric Capacity Metrics semantic model
# 2. `METRIC_DATASET_ID` - semantic model ID
# 3. `TARGET_CAPACITY_ID` - default capacity to analyse
# 4. `LAKEHOUSE_NAME` - target lakehouse
# 5. `DEFAULT_START_DATE_STR` / `DEFAULT_END_DATE_STR` - shared collection window
# 6. `DISPLAY_DATA` - debug output flag
# 
# ### Important rules
# - `%run` must be the only statement in its cell
# - Keep `DISPLAY_DATA = False` for operations; enable it only for debugging
# - The semantic model and lakehouse do not need to live in the same workspace
# - `0300` is the only notebook that writes curated `gold` tables
# - `0220` and `0230` stage pandas results into the shared temp persisted store before one final MERGE
# 
# ## Output tables
# | Layer | Table |
# |---|---|
# | `silver` | `capacities` |
# | `silver` | `capacity_metrics_timepoints` |
# | `silver` | `timepoint_metrics` |
# | `silver` | `timepoint_metrics_autoscale` |
# | `gold` | `capacities` |
# | `gold` | `capacity_metrics_by_timepoint` |
# | `gold` | `capacity_calendar_timepoints` |
# | `gold` | `timepoint_metrics` |
# | `gold` | `timepoint_metrics_autoscale` |
# 
# ## Troubleshooting
# ### `0220` or `0230` fails immediately
# Confirm the prep `0300` run succeeded and `gold.capacity_calendar_timepoints` exists for the requested range.
# 
# ### Pipeline run fails before notebook execution starts
# Check activity names first. Fabric rejects activity names longer than 60 characters.
# 
# ### Scheduled run cannot resolve lakehouse-qualified names
# Use shared helpers from `0050` such as `local_table_name()` and `table_exists()`.
# 
# ### Autoscale totals look too low
# Use `gold.timepoint_metrics_autoscale.Acc_CU_s` for cumulative CU.
# `SumCU_s` is only the point-in-time value for a single timepoint.
# 
# ### Duplicate-row concerns
# The silver and gold writers deduplicate source batches before the final MERGE. Re-runs are supported.
# 
# ## Consumption notebook
# Use `nb_0400_analyze_cu_consumption_patterns` for interactive analysis.
# It contains the common CU-consumption patterns plus the `vw_item_consumption_by_operation_id` temporary view.


# CELL ********************

%run "./nb_0110_shared_config"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('nb_0100')

PIPELINE_BLUEPRINTS = {
    'capacity': [
        'Run_0200_CollectCapacityMetrics',
        'Run_0300_GenerateGoldTables_CalendarPrep',
        'Run_0220_CollectCapacityMetricsTimepointDetail',
        'Run_0300_GenerateGoldTables_Final',
    ],
    'autoscale': [
        'Run_0200_CollectCapacityMetrics',
        'Run_0300_GenerateGoldTables_CalendarPrep',
        'Run_0230_CollectAutoscaleDetail',
        'Run_0300_GenerateGoldTables_Final',
    ],
    'both': [
        'Run_0200_CollectCapacityMetrics',
        'Run_0300_GenerateGoldTables_CalendarPrep',
        'Run_0220_CollectCapacityMetricsTimepointDetail',
        'Run_0230_CollectAutoscaleDetail',
        'Run_0300_GenerateGoldTables_Final',
    ],
}

logger.info('Guide notebook loaded. Use the Fabric Data Pipelines or run the downstream notebooks manually.')
logger.info('Shared config | model_workspace=%s | model_dataset=%s | capacity=%s | range=%s -> %s',
            METRIC_WORKSPACE_ID, METRIC_DATASET_ID, TARGET_CAPACITY_ID,
            DEFAULT_START_DATE_STR, DEFAULT_END_DATE_STR)
for mode_name, activity_names in PIPELINE_BLUEPRINTS.items():
    logger.info('Pipeline blueprint [%s]: %s', mode_name, ' -> '.join(activity_names))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }


