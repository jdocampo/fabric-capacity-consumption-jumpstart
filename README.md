# Fabric Capacity Consumption Jumpstart

Deployable Microsoft Fabric Jumpstart content for extracting, persisting, and analyzing Microsoft Fabric Capacity Metrics consumption data. The jumpstart installs a Lakehouse, notebooks, and a parameterized Data Pipeline into a Fabric workspace folder named `capacity-consumption`.

> **Important:** This solution extracts data from the Fabric Capacity Metrics semantic model by issuing DAX queries from notebooks. That is an unsupported extraction path for Capacity Metrics app data. Use it at your own risk and validate the results in your environment.

## What gets installed

| Item | Type | Purpose |
| --- | --- | --- |
| `capacity_metrics` | Lakehouse | Stores silver and gold Capacity Metrics tables. |
| `get_started` | Notebook | Entry point with requirements, architecture, table model, and operating guidance. |
| `shared_config` | Notebook | Shared configuration for semantic model placeholders, target capacity, Lakehouse, schemas, table names, and helper functions. |
| `collect_capacity_metrics` | Notebook | Collects capacity metric timepoints and capacity attributes. |
| `collect_timepoint_detail` | Notebook | Collects standard activity detail by capacity/timepoint. |
| `collect_autoscale_timepoint_detail` | Notebook | Collects autoscale activity detail by capacity/timepoint. |
| `generate_gold_tables` | Notebook | Builds curated gold tables from silver extracts. |
| `analyze_cu_consumption_patterns` | Notebook | Analyzes CU consumption patterns over the generated gold tables. |
| `orchestrate_capacity_consumption` | Data Pipeline | Runs collection and gold table generation tasks. |

## Requirements

- Access to a Microsoft Fabric workspace with active capacity.
- The Microsoft Fabric Capacity Metrics app installed and refreshed.
- The workspace and semantic model IDs for the Capacity Metrics semantic model created by that app.
- The capacity ID to analyze.
- Permission to query the semantic model and create/run Fabric items in the target workspace.

After installation, open `shared_config` and replace:

- `<capacity-metrics-workspace-id>`
- `<capacity-metrics-semantic-model-id>`
- `<target-capacity-id>`

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
    repo_ref="v0.1.0",
    entry_point="get_started.Notebook",
    items_in_scope=["Lakehouse", "Notebook", "DataPipeline"],
    workspace_path="workspace/",
    name="Capacity Consumption",
    workspace_id="<target-workspace-id>",
)
```

## Run the solution

1. Open `get_started`.
2. Review `shared_config` and replace semantic model/capacity placeholders.
3. Run `orchestrate_capacity_consumption`.
4. Inspect silver and gold tables in the `capacity_metrics` Lakehouse.
5. Use `analyze_cu_consumption_patterns` for exploratory analysis.

## Data model

The solution writes raw semantic model extracts into silver Delta tables and curated analytical outputs into gold Delta tables.

![Capacity Metrics ERD](https://github.com/jdocampo/fabric-capacity-consumption-jumpstart/blob/main/assets/images/capacity_metrics_erd.png?raw=true)

## Repository structure

```text
assets/
  images/
    capacity_metrics_erd.png
workspace/
  parameter.yml
  capacity-consumption/
    *.Lakehouse/
    *.Notebook/
    *.DataPipeline/
```

`workspace/parameter.yml` replaces deployed Lakehouse, Notebook, pipeline activity, and workspace references at install time so source workspace IDs are not reused for deployed items.
