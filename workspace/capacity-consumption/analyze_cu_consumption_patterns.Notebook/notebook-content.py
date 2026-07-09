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

# # analyze_cu_consumption_patterns
# 
# ## Purpose
# This notebook consumes the curated gold tables and surfaces the most common CU-consumption analysis patterns.
# 
# ## Included patterns
# 1. Capacity utilization trend by timepoint
# 2. Top items by CU consumption
# 3. Top workspaces by CU consumption
# 4. Top operations by CU consumption
# 5. Autoscale cumulative CU analysis
# 6. Item inspection view by `Operation_Id`

# CELL ********************

%run "./shared_config"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

target_capacity_id = TARGET_CAPACITY_ID
start_date_str = DEFAULT_START_DATE_STR
end_date_str = DEFAULT_END_DATE_STR
operation_id_filter = ''
top_n = 20
include_autoscale = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

top_n = int(top_n)
if isinstance(include_autoscale, str):
    include_autoscale = include_autoscale.strip().lower() in ('1', 'true', 'yes', 'y')
operation_id_filter = str(operation_id_filter).strip()

required_tables = [GOLD_METRICS_BY_TIMEPOINT, GOLD_CALENDAR_TIMEPOINTS, GOLD_TP_METRICS]
missing_tables = [table_name for table_name in required_tables if not table_exists(table_name)]
if missing_tables:
    raise RuntimeError(f"Missing required gold tables: {missing_tables}. Run one of the `orchestrate_capacity_consumption` Data Pipelines or manually run `collect_capacity_metrics -> generate_gold_tables -> collect_timepoint_detail/collect_autoscale_timepoint_detail -> generate_gold_tables` first.")

range_filter = f"TimePoint BETWEEN to_timestamp('{start_date_str}', 'dd MMMM yyyy HH:mm') AND to_timestamp('{end_date_str}', 'dd MMMM yyyy HH:mm')"
capacity_filter = f"CapacityId = '{target_capacity_id}'"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

capacity_metrics_df = spark.sql(f"""
SELECT *
FROM {GOLD_METRICS_BY_TIMEPOINT}
WHERE {capacity_filter} AND {range_filter}
""")
capacity_metrics_df.createOrReplaceTempView('vw_capacity_metrics_filtered')

standard_detail_df = spark.sql(f"""
SELECT *
FROM {GOLD_TP_METRICS}
WHERE {capacity_filter} AND {range_filter}
""")
standard_detail_df.createOrReplaceTempView('vw_standard_item_consumption')

autoscale_available = include_autoscale and table_exists(GOLD_TP_METRICS_AS)
if autoscale_available:
    autoscale_detail_df = spark.sql(f"""
    SELECT *
    FROM {GOLD_TP_METRICS_AS}
    WHERE {capacity_filter} AND {range_filter}
    """)
else:
    autoscale_detail_df = None

standard_view_df = standard_detail_df.select(
    F.lit('standard').alias('SourceType'),
    'CapacityId', 'TimePoint', 'Operation_Id', 'Operation', 'Workspace_name', 'Item_name', 'Item_kind', 'User', 'Unique_key',
    F.col('SumTotal_CU_s').alias('PointInTimeCU_s'),
    F.lit(None).cast('double').alias('CumulativeCU_s')
)

if autoscale_detail_df is not None:
    autoscale_view_df = autoscale_detail_df.select(
        F.lit('autoscale').alias('SourceType'),
        'CapacityId', 'TimePoint', 'Operation_Id', 'Operation', 'Workspace_name', 'Item_name', 'Item_kind', 'User', 'Unique_key',
        F.col('SumCU_s').alias('PointInTimeCU_s'),
        F.col('Acc_CU_s').alias('CumulativeCU_s')
    )
    operation_view_df = standard_view_df.unionByName(autoscale_view_df)
else:
    operation_view_df = standard_view_df

operation_view_df.createOrReplaceTempView('vw_item_consumption_by_operation_id')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Pattern 1 - Capacity utilization trend
# 
# Use this view to understand how the selected capacity behaved over time in the selected range.

# CELL ********************

display(spark.sql(f"""
SELECT
    TimePoint,
    TotalCUs,
    TotalCUUsagePercentage,
    BackgroundPercentage,
    InteractivePercentage,
    AutoscalePercentage,
    CarryOverCumulativePercentage
FROM vw_capacity_metrics_filtered
ORDER BY TimePoint
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Pattern 2 - Top items by CU consumption

# CELL ********************

display(spark.sql(f"""
SELECT
    Workspace_name,
    Item_name,
    Item_kind,
    SUM(SumTotal_CU_s) AS TotalCU_s,
    COUNT(*) AS Samples
FROM vw_standard_item_consumption
GROUP BY Workspace_name, Item_name, Item_kind
ORDER BY TotalCU_s DESC
LIMIT {top_n}
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql(f"""
SELECT
    Workspace_name,
    SUM(SumTotal_CU_s) AS TotalCU_s,
    COUNT(DISTINCT Item_name) AS DistinctItems,
    COUNT(DISTINCT Operation_Id) AS DistinctOperations
FROM vw_standard_item_consumption
GROUP BY Workspace_name
ORDER BY TotalCU_s DESC
LIMIT {top_n}
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql(f"""
SELECT
    Operation,
    SUM(SumTotal_CU_s) AS TotalCU_s,
    COUNT(DISTINCT Operation_Id) AS DistinctOperationIds,
    COUNT(DISTINCT Item_name) AS DistinctItems
FROM vw_standard_item_consumption
GROUP BY Operation
ORDER BY TotalCU_s DESC
LIMIT {top_n}
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Pattern 5 - Autoscale cumulative CU
# 
# For autoscale, `SumCU_s` is the point-in-time value and `Acc_CU_s` is the cumulative value across timepoints for the same operation.
# 
# This pattern returns the latest autoscale observation for each `Operation_Id` and `Unique_key`, so each row represents the current cumulative CU state for one concrete item-operation instance rather than every intermediate timepoint.

# CELL ********************

inspection_sql = f"""
SELECT a.*
FROM vw_item_consumption_by_operation_id a
JOIN (
    SELECT Operation, Operation_Id, MAX(TimePoint) AS MaxTimePoint, Unique_key
    FROM {GOLD_TP_METRICS_AS}
    GROUP BY Operation, Operation_Id, Unique_key
) b 
ON a.Operation_Id = b.Operation_Id AND a.TimePoint = b.MaxTimePoint AND a.Unique_key = b.Unique_key
WHERE SourceType = 'autoscale'
"""
if operation_id_filter:
    inspection_sql += f" WHERE a.Operation_Id = '{operation_id_filter}'"
inspection_sql += " ORDER BY a.TimePoint DESC, a.Item_name"
df = spark.sql(inspection_sql)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Pattern 6 - Item inspection by operation id
# 
# This notebook creates the temporary view `vw_item_consumption_by_operation_id`. Set `operation_id_filter` to inspect one operation, or leave it blank to inspect the full result set.

# CELL ********************

inspection_sql = "SELECT * FROM vw_item_consumption_by_operation_id"
if operation_id_filter:
    inspection_sql += f" WHERE Operation_Id = '{operation_id_filter}'"
inspection_sql += " ORDER BY TimePoint DESC, SourceType, Item_name"
display(spark.sql(inspection_sql))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }





