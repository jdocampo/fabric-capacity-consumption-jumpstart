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

# # Autoscale Validation
# 
# Ad hoc validation notebook to confirm autoscale silver/gold visibility after collector and gold generation runs.

# CELL ********************

%run "./nb_0110_shared_config"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import logging
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('nb_0420_validate_autoscale')

print('CATALOG_DATABASES', json.dumps([row.asDict(recursive=True) for row in spark.sql('SHOW DATABASES').collect()], default=str))
for schema_name in (SILVER_SCHEMA, GOLD_SCHEMA):
    try:
        tables = [row.asDict(recursive=True) for row in spark.sql(f'SHOW TABLES IN {schema_name}').collect()]
        print(f'CATALOG_TABLES_{schema_name.upper()}', json.dumps(tables, default=str))
    except Exception as ex:
        print(f'CATALOG_TABLES_{schema_name.upper()}_ERROR', str(ex))

for table_name in (SILVER_TP_METRICS_AS, GOLD_TP_METRICS_AS, local_table_name(SILVER_TP_METRICS_AS), local_table_name(GOLD_TP_METRICS_AS)):
    try:
        print('DIRECT_TABLE_CHECK', table_name, spark.table(table_name).limit(1).count())
    except Exception as ex:
        print('DIRECT_TABLE_CHECK_ERROR', table_name, str(ex))

required_tables = [SILVER_TP_METRICS_AS, GOLD_TP_METRICS_AS]
missing_tables = [table_name for table_name in required_tables if not table_exists(table_name)]
summary = {'missing_tables': missing_tables}
sample_rows = []
if missing_tables:
    print('AUTOSCALE_VALIDATION_MISSING_TABLES', json.dumps(missing_tables))
else:
    for label, table_name in [('silver', SILVER_TP_METRICS_AS), ('gold', GOLD_TP_METRICS_AS)]:
        df = spark.table(table_name)
        summary[label] = {
            'row_count': df.count(),
            'synthetic_operation_id_count': df.filter(F.col('Operation_Id').like('synthetic-%')).count(),
            'latest_timepoint': df.agg(F.max('TimePoint').alias('latest')).collect()[0]['latest'],
        }

    sample_rows = [
        row.asDict(recursive=True)
        for row in spark.table(GOLD_TP_METRICS_AS)
            .select('CapacityId', 'TimePoint', 'Operation_Id', 'Operation', 'Item_name', 'SumCU_s', 'Acc_CU_s')
            .orderBy(F.col('TimePoint').desc())
            .limit(10)
            .collect()
    ]

print('AUTOSCALE_VALIDATION_SUMMARY', json.dumps(summary, default=str))
print('AUTOSCALE_VALIDATION_SAMPLE', json.dumps(sample_rows, default=str))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }


