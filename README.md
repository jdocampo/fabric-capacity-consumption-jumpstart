# Fabric Capacity Consumption Jumpstart

Deployable Microsoft Fabric Jumpstart content for collecting and analyzing Capacity Metrics consumption data. The jumpstart installs a Lakehouse, administration/configuration notebooks, consumption analysis notebooks, and a parameterized Data Pipeline into a Fabric workspace folder named `capacity-consumption`.

## What gets installed

| Item | Type | Purpose |
| --- | --- | --- |
| `capacity_metrics` | Lakehouse | Stores silver and gold Capacity Metrics tables. |
| `nb_0100_get_started` | Notebook | Entry point with deployment guidance and next steps. |
| `nb_0110_shared_config` | Notebook | Shared configuration for model, capacity, Lakehouse, and table names. |
| `nb_0200_admin_orchestrator` | Notebook | Notebook-based orchestration helper. |
| `nb_0210_collect_capacity_metrics` | Notebook | Collects capacity metric timepoints. |
| `nb_0220_collect_timepoint_detail` | Notebook | Collects standard timepoint activity detail. |
| `nb_0230_collect_timepoint_detail_parallel` | Notebook | Parallel variant for detailed timepoint collection. |
| `nb_0240_collect_autoscale_timepoint_detail` | Notebook | Collects autoscale timepoint detail. |
| `nb_0250_explore_capacity_units` | Notebook | Explores capacity unit metrics. |
| `nb_0260_explore_autoscale_capacity_units` | Notebook | Explores autoscale capacity unit metrics. |
| `nb_0300_generate_gold_tables` | Notebook | Builds curated gold tables. |
| `nb_0400_analyze_cu_consumption_patterns` | Notebook | Analyzes CU consumption patterns. |
| `nb_0420_validate_autoscale` | Notebook | Validates autoscale visibility. |
| `pl_0100_orchestrate_capacity_consumption` | Data Pipeline | Runs the collection and gold table generation flow. |

## Prerequisites

- Access to a Microsoft Fabric workspace with active capacity.
- A Capacity Metrics Semantic Model that already exists outside this jumpstart.
- Permission to install Fabric items into the target workspace.

The default shared configuration currently points to the existing source Capacity Metrics Semantic Model. Review `nb_0110_shared_config` after installation and update `METRIC_WORKSPACE_ID`, `METRIC_DATASET_ID`, and `TARGET_CAPACITY_ID` if your semantic model or target capacity differs.

## Install from GitHub

Create a Fabric notebook in the target workspace, then run:

```python
%pip install fabric-jumpstart --quiet
```

```python
import fabric_jumpstart as jumpstart

jumpstart._install_from_github(
    logical_id="capacity-consumption",
    repo_url="https://github.com/jdocampo/fabric-capacity-consumption-jumpstart.git",
    repo_ref="main",
    entry_point="nb_0100_get_started.Notebook",
    items_in_scope=["Lakehouse", "Notebook", "DataPipeline"],
    workspace_path="workspace/",
    name="Capacity Consumption",
    workspace_id="<target-workspace-guid>",
)
```

For validation in the `jodocamp` workspace, use workspace ID `a8168a9f-1fe8-4029-a5b6-ba3d75bac4a9`.

## Run the solution

1. Open `nb_0100_get_started`.
2. Review `nb_0110_shared_config` and adjust semantic model or capacity IDs if needed.
3. Run `pl_0100_orchestrate_capacity_consumption`.
4. Inspect silver and gold tables in the `capacity_metrics` Lakehouse.
5. Use `nb_0400_analyze_cu_consumption_patterns` and the exploration notebooks for analysis.

## Repository structure

```text
workspace/
  parameter.yml
  capacity-consumption/
    *.Lakehouse/
    *.Notebook/
    *.DataPipeline/
```

`workspace/parameter.yml` replaces deployed Lakehouse, Notebook, pipeline activity, and workspace references at install time so the source workspace IDs are not reused for deployed items.
