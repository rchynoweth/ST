# Databricks notebook source
# MAGIC %md
# MAGIC # System Table ETL 
# MAGIC
# MAGIC System tables provided by Databricks have a 12 month retention period. This is a DLT pipeline that can be executed daily to archive system data to another schema. 
# MAGIC
# MAGIC To use this pipeline, simply add the tables to the `tables` python list object and it will incrementally load the table. 
# MAGIC
# MAGIC Create a DLT pipeline by following these [instructions](https://docs.databricks.com/delta-live-tables/tutorial-pipelines.html).

# COMMAND ----------

dbutils.widgets.text('TargetCatalog', 'main')
dbutils.widgets.text('TargetSchema', 'system_tables')
dbutils.widgets.text('CheckpointLocations', '')
target_catalog = dbutils.widgets.get('TargetCatalog')
target_schema = dbutils.widgets.get('TargetSchema')
checkpoint_location = dbutils.widgets.get('CheckpointLocations')

# COMMAND ----------

spark.sql(f'use catalog {target_catalog}')
spark.sql(f'create schema if not exists {target_schema}')
spark.sql(f'use schema {target_schema}')

# COMMAND ----------

tables = [
  'system.billing.usage'
  , 'system.access.audit'
]

# COMMAND ----------

# Billing Stream
billing_df = (spark.readStream
  .format("deltaSharing")
  .table("system.billing.usage")
)


# COMMAND ----------

display(billing_df)

# COMMAND ----------

(
  billing_df  
  .writeStream
  .format("delta")
  .option("checkpointLocation", f"{checkpoint_location}/system_billing_usage")
  .trigger(availableNow=True)
  .toTable("system_billing_usage")
)
