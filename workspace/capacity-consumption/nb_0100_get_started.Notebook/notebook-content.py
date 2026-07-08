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

# # Capacity Consumption Jumpstart
#
# Welcome to the Capacity Consumption jumpstart. This solution deploys the Lakehouse, notebooks, and orchestrator pipeline used to collect, prepare, and analyze Microsoft Fabric capacity consumption data.
#
# ## Deployed items
#
# - `capacity_metrics` Lakehouse
# - `nb_0110_shared_config` configuration notebook
# - `nb_0200_admin_orchestrator` administration orchestrator notebook
# - `nb_0210_collect_capacity_metrics` metrics collection notebook
# - `nb_0220_collect_timepoint_detail` detailed timepoint collection notebook
# - `nb_0230_collect_timepoint_detail_parallel` parallel detailed timepoint collection notebook
# - `nb_0240_collect_autoscale_timepoint_detail` autoscale detail collection notebook
# - `nb_0250_explore_capacity_units` capacity unit exploration notebook
# - `nb_0260_explore_autoscale_capacity_units` autoscale capacity unit exploration notebook
# - `nb_0300_generate_gold_tables` gold table generation notebook
# - `nb_0400_analyze_cu_consumption_patterns` consumption pattern analysis notebook
# - `nb_0420_validate_autoscale` autoscale validation notebook
# - `pl_0100_orchestrate_capacity_consumption` orchestrator pipeline
#
# ## Prerequisites
#
# The Capacity Metrics Semantic Model is not deployed by this jumpstart. It must already exist in the appropriate workspace before you configure or run downstream reporting scenarios.
#
# ## Recommended flow
#
# 1. Open `nb_0110_shared_config` and review configuration values for your tenant and capacity metrics scenario.
# 2. Run `pl_0100_orchestrate_capacity_consumption` to execute the collection and transformation flow.
# 3. Review generated tables in the `capacity_metrics` Lakehouse.
# 4. Use `nb_0400_analyze_cu_consumption_patterns` and related exploration notebooks to inspect consumption patterns.
#
# ## Notes
#
# This notebook is intentionally informational. It does not run the pipeline automatically because Fabric Jumpstarts should leave execution actions explicit for the user.

# CELL ********************

print("Capacity Consumption jumpstart installed. Start with nb_0110_shared_config, then run pl_0100_orchestrate_capacity_consumption.")


