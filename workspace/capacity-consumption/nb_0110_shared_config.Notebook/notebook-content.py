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

# # nb_0110_shared_config
# 
# ## Purpose
# Centralised configuration shared by ALL Capacity Metrics notebooks.
# Defines workspace/model/lakehouse/schema/table constants in exactly one place.
# 
# **Usage:** Add `%run "./nb_0110_shared_config"` as the first code cell in each notebook.
# After the `%run`, override individual parameters in the notebook's own `parameters` cell.
# 
# **This notebook does NOT run any orchestration or data movement.**
# It defines shared constants and small table-name helpers used by the other notebooks.
# 
# ## Variables Exported
# | Variable | Description |
# |---|---|
# | METRIC_WORKSPACE_ID | Workspace hosting the Fabric Capacity Metrics semantic model |
# | METRIC_DATASET_ID | Semantic model (dataset) ID |
# | TARGET_CAPACITY_ID | Default capacity to analyse |
# | LAKEHOUSE_NAME | Lakehouse name |
# | SILVER_SCHEMA / GOLD_SCHEMA | Schema names |
# | SILVER_CAPACITIES | silver.capacities |
# | SILVER_TIMEPOINTS | silver.capacity_metrics_timepoints |
# | SILVER_TP_METRICS | silver.timepoint_metrics |
# | SILVER_TP_METRICS_AS | silver.timepoint_metrics_autoscale |
# | GOLD_CAPACITIES | gold.capacities |
# | GOLD_METRICS_BY_TIMEPOINT | gold.capacity_metrics_by_timepoint |
# | GOLD_CALENDAR_TIMEPOINTS | gold.capacity_calendar_timepoints |
# | GOLD_TP_METRICS | gold.timepoint_metrics |
# | GOLD_TP_METRICS_AS | gold.timepoint_metrics_autoscale (includes Acc_CU_s) |
# | DEFAULT_START_DATE_STR / DEFAULT_END_DATE_STR | Default shared collection window for 0100 / 0200 / 0220 / 0230 |
# | DISPLAY_DATA | Default verbose flag |
# | create_temp_persisted_store / append_pandas_to_temp_store / load_temp_store_to_spark / delete_temp_persisted_store | Shared non-Spark temp staging helpers |
# 
# ## Notes
# - The semantic model may live in a **different workspace** from the lakehouse.
# - Referenced by: nb_0200_admin_orchestrator, 0200, 0220, 0230, 0300, 0240, 0250.


# CELL ********************

# ==============================================================
# Semantic model -- supports model in a DIFFERENT workspace from the lakehouse
# ==============================================================
METRIC_WORKSPACE_ID = "cb2325c3-50bc-40bf-91c8-49effb0200d8"
METRIC_DATASET_ID   = "8a96adfa-aa44-4a8a-ac95-80bf42d87d00"

# ==============================================================
# Default target capacity (override in any notebook parameter cell after %run)
# ==============================================================
TARGET_CAPACITY_ID  = "945EDF1D-4BF7-426F-AAF8-9B77C65AA00A"

# ==============================================================
# Lakehouse and schema layout
# ==============================================================
LAKEHOUSE_NAME = "capacity_metrics"
SILVER_SCHEMA  = "silver"
GOLD_SCHEMA    = "gold"

# Silver tables -- raw collected data (persisted, never deleted)
SILVER_CAPACITIES    = f"{LAKEHOUSE_NAME}.{SILVER_SCHEMA}.capacities"
SILVER_TIMEPOINTS    = f"{LAKEHOUSE_NAME}.{SILVER_SCHEMA}.capacity_metrics_timepoints"
SILVER_TP_METRICS    = f"{LAKEHOUSE_NAME}.{SILVER_SCHEMA}.timepoint_metrics"
SILVER_TP_METRICS_AS = f"{LAKEHOUSE_NAME}.{SILVER_SCHEMA}.timepoint_metrics_autoscale"

# Gold tables -- curated outputs written by nb_0300_generate_gold_tables
GOLD_CAPACITIES           = f"{LAKEHOUSE_NAME}.{GOLD_SCHEMA}.capacities"
GOLD_METRICS_BY_TIMEPOINT = f"{LAKEHOUSE_NAME}.{GOLD_SCHEMA}.capacity_metrics_by_timepoint"
GOLD_CALENDAR_TIMEPOINTS  = f"{LAKEHOUSE_NAME}.{GOLD_SCHEMA}.capacity_calendar_timepoints"
GOLD_TP_METRICS           = f"{LAKEHOUSE_NAME}.{GOLD_SCHEMA}.timepoint_metrics"
GOLD_TP_METRICS_AS        = f"{LAKEHOUSE_NAME}.{GOLD_SCHEMA}.timepoint_metrics_autoscale"

# ==============================================================
# Collection defaults (overrideable via each notebook's parameters cell)
# ==============================================================
DEFAULT_START_DATE_STR = "05 June 2026 07:30"
DEFAULT_END_DATE_STR   = "05 June 2026 20:59"
DISPLAY_DATA          = False               # set True for interactive debugging only

import os
import shutil
import threading
import time
import uuid
import pyarrow as pa
import pyarrow.parquet as pq

TEMP_STORE_ROOT_LOCAL = "/lakehouse/default/Files/tmp/capacity_metrics"
TEMP_STORE_ROOT_SPARK = "Files/tmp/capacity_metrics"

def local_table_name(table_name):
    parts = table_name.split('.')
    return '.'.join(parts[1:]) if len(parts) == 3 and parts[0] == LAKEHOUSE_NAME else table_name


def split_table_name(table_name):
    parts = local_table_name(table_name).split('.')
    if len(parts) == 2:
        return parts[0], parts[1]
    if len(parts) == 1:
        return None, parts[0]
    raise ValueError(f"Unsupported table name format: {table_name}")


def table_exists(table_name):
    schema_name, object_name = split_table_name(table_name)
    if schema_name is None:
        return spark.catalog.tableExists(object_name)
    return spark.sql(f"SHOW TABLES IN {schema_name} LIKE '{object_name}'").count() > 0


def to_python_datetime(value):
    return value.to_pydatetime() if hasattr(value, 'to_pydatetime') else value


def create_temp_persisted_store(store_label):
    store_id = f"{store_label}_{uuid.uuid4().hex}"
    local_path = os.path.join(TEMP_STORE_ROOT_LOCAL, store_id)
    spark_path = f"{TEMP_STORE_ROOT_SPARK}/{store_id}"
    if os.path.exists(local_path):
        shutil.rmtree(local_path)
    os.makedirs(local_path, exist_ok=True)
    return {
        "store_id": store_id,
        "local_path": local_path,
        "spark_path": spark_path,
        "parts_written": 0,
        "lock": threading.Lock(),
    }


def append_pandas_to_temp_store(store_state, pandas_df):
    if pandas_df is None or pandas_df.empty:
        return 0
    with store_state["lock"]:
        part_number = store_state["parts_written"]
        part_path = os.path.join(store_state["local_path"], f"part_{part_number:05d}.parquet")
        table = pa.Table.from_pandas(pandas_df, preserve_index=False)
        pq.write_table(
            table,
            part_path,
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
        )
        store_state["parts_written"] = part_number + 1
        return len(pandas_df.index)


def load_temp_store_to_spark(store_state):
    if store_state["parts_written"] == 0:
        return None
    return spark.read.parquet(store_state["spark_path"])


def delete_temp_persisted_store(store_state):
    if not store_state:
        return
    spark_path = store_state["spark_path"]
    local_path = store_state["local_path"]
    deleted_via_fabric = False
    last_error = None
    if "mssparkutils" in globals():
        for _ in range(5):
            try:
                if mssparkutils.fs.exists(spark_path):
                    mssparkutils.fs.rm(spark_path, True)
                deleted_via_fabric = True
                last_error = None
                break
            except Exception as ex:
                last_error = ex
                time.sleep(2)
        if last_error is not None:
            raise RuntimeError(f"Failed to delete temporary staged store via Fabric path {spark_path}: {last_error}") from last_error
    if deleted_via_fabric and local_path.startswith(TEMP_STORE_ROOT_LOCAL):
        return
    if os.path.exists(local_path):
        for _ in range(5):
            try:
                shutil.rmtree(local_path)
                last_error = None
                break
            except OSError as ex:
                last_error = ex
                time.sleep(2)
        if last_error is not None and os.path.exists(local_path):
            raise RuntimeError(f"Failed to delete temporary staged store via local path {local_path}: {last_error}") from last_error

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }


