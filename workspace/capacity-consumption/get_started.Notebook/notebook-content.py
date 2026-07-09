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
# META       "default_lakehouse_workspace_id": "cb2325c3-50bc-40bf-91c8-49effb0200d8"
# META     }
# META   }
# META }

# MARKDOWN ********************

# <div style="margin: 0; padding: 0; text-align: left;">
#   <table style="border: none; margin: 0; padding: 0; border-collapse: collapse;">
#     <tr>
#       <td style="border: none; vertical-align: middle; padding: 0 16px 0 0;">
#         <img src="https://github.com/jdocampo/fabric-capacity-consumption-jumpstart/blob/main/assets/images/capacity_metrics_erd.png?raw=true" width="190" />
#       </td>
#       <td style="border: none; vertical-align: middle; padding: 0;">
#         <h1 style="font-weight: bold; margin: 0;">Capacity Consumption Jumpstart</h1>
#         <p style="margin: 4px 0 0 0;">Extract, persist, and analyze Microsoft Fabric Capacity Metrics data in a Lakehouse.</p>
#       </td>
#     </tr>
#   </table>
# </div>
#
# ## Purpose
#
# This jumpstart installs a small Fabric data engineering solution that reads Microsoft Fabric Capacity Metrics data from an existing **Fabric Capacity Metrics semantic model**, stores raw extracts in a Lakehouse silver layer, and creates curated gold tables for downstream analysis.
#
# Use it when you need a repeatable way to capture Capacity Metrics slices over a chosen time window, persist them as Delta tables, and inspect consumption patterns beyond the standard app experience.
#
# > [!WARNING]
# > This solution extracts data from the Capacity Metrics semantic model by issuing DAX queries from notebooks. This is an **unsupported extraction path** for the Capacity Metrics app data. It is provided as a jumpstart for exploration and engineering acceleration only. Use it at your own risk, validate results for your environment, and do not treat it as a Microsoft-supported product interface.
#
# ## Requirements
#
# - Install the **Microsoft Fabric Capacity Metrics app** and ensure it is refreshed.
# - Identify the workspace and semantic model IDs for the Capacity Metrics semantic model created by the app.
# - Identify the capacity ID that you want to analyze.
# - Confirm you have permission to query the semantic model and to create/run Fabric notebooks, pipelines, and Lakehouse tables in this workspace.
# - Review `shared_config` and replace the placeholder values before running collection notebooks or the pipeline:
#   - `<capacity-metrics-workspace-id>`
#   - `<capacity-metrics-semantic-model-id>`
#   - `<target-capacity-id>`
#
# ## Installed items
#
# | Item | Type | Purpose |
# |---|---|---|
# | `capacity_metrics` | Lakehouse | Stores silver and gold Delta tables. |
# | `get_started` | Notebook | This entry point and operating guide. |
# | `shared_config` | Notebook | Central constants, table names, semantic model identifiers, target capacity, and shared helper functions. |
# | `collect_capacity_metrics` | Notebook | Queries capacity-level metric timepoints and writes `silver.capacities`, `silver.capacity_metrics_timepoints`, and calendar timepoints. |
# | `collect_timepoint_detail` | Notebook | Queries detailed activity metrics for standard capacity consumption timepoints and writes `silver.timepoint_metrics`. |
# | `collect_autoscale_timepoint_detail` | Notebook | Queries autoscale activity detail and writes `silver.timepoint_metrics_autoscale`. |
# | `generate_gold_tables` | Notebook | Builds curated gold tables from silver data. |
# | `analyze_cu_consumption_patterns` | Notebook | Provides exploratory analysis over the generated gold tables. |
# | `orchestrate_capacity_consumption` | Data Pipeline | Runs collection notebooks and final gold table generation in sequence. |
#
# ## Orchestration flow
#
# The `orchestrate_capacity_consumption` Data Pipeline runs four notebook activities:
#
# 1. `collect_capacity_metrics` extracts capacity-level timepoint data for the configured date window and target capacity. It also records the timepoints used by downstream detail extraction.
# 2. `collect_timepoint_detail` runs after capacity metrics collection and extracts standard timepoint activity detail.
# 3. `collect_autoscale_timepoint_detail` runs after capacity metrics collection and extracts autoscale-related activity detail.
# 4. `generate_gold_tables` runs after both detail collection tasks succeed and rebuilds curated gold outputs.
#
# The pipeline does not run automatically when the jumpstart is installed. Open it and run it explicitly after you review `shared_config`.
#
# ## Lakehouse table model
#
# The solution follows a simple silver/gold pattern:
#
# - **Silver tables** keep raw, append-friendly extracts from the semantic model. They are designed for traceability and reprocessing.
# - **Gold tables** are curated outputs for analysis. They align capacity, calendar/timepoint, standard activity, and autoscale activity data.
#
# ![Capacity Metrics ERD](https://github.com/jdocampo/fabric-capacity-consumption-jumpstart/blob/main/assets/images/capacity_metrics_erd.png?raw=true)
#
# ### Silver tables
#
# | Table | Description |
# |---|---|
# | `silver.capacities` | Capacity list and descriptive attributes extracted from the semantic model. |
# | `silver.capacity_metrics_timepoints` | Capacity-level metric rows by timepoint for the configured window. |
# | `silver.timepoint_metrics` | Standard activity detail by capacity and timepoint. |
# | `silver.timepoint_metrics_autoscale` | Autoscale activity detail by capacity and timepoint, including accumulated CU seconds. |
#
# ### Gold tables
#
# | Table | Description |
# |---|---|
# | `gold.capacities` | Curated capacity dimension. |
# | `gold.capacity_calendar_timepoints` | Timepoint/calendar helper table used to align time series analysis. |
# | `gold.capacity_metrics_by_timepoint` | Curated capacity-level facts by timepoint. |
# | `gold.timepoint_metrics` | Curated standard activity detail facts. |
# | `gold.timepoint_metrics_autoscale` | Curated autoscale activity detail facts. |
#
# ## Recommended operating sequence
#
# 1. Open `shared_config`.
# 2. Replace placeholder semantic model and capacity values.
# 3. Choose `DEFAULT_START_DATE_STR` and `DEFAULT_END_DATE_STR` values that match the Capacity Metrics app data retention window.
# 4. Run `orchestrate_capacity_consumption`.
# 5. Inspect the tables in `capacity_metrics`.
# 6. Open `analyze_cu_consumption_patterns` for exploratory analysis.
#
# ## Practical guidance
#
# - Start with a short time window to validate permissions and semantic model query behavior.
# - Keep the Capacity Metrics app refreshed; stale app data produces stale Lakehouse extracts.
# - Re-run `generate_gold_tables` after any silver-layer backfill or correction.
# - Treat DAX/query failures as signals to check semantic model permissions, renamed app artifacts, app refresh state, and date-window availability.
# - If you adapt the notebooks, keep semantic model identifiers centralized in `shared_config` rather than copying them across notebooks.

# CELL ********************

print("Capacity Consumption jumpstart installed. Review shared_config, then run orchestrate_capacity_consumption.")


