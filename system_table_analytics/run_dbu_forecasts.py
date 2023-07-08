# Databricks notebook source
# MAGIC %md
# MAGIC # Run DBU Forecasts
# MAGIC
# MAGIC This notebook sources data from Databricks System Tables (`system.billing.usage`). Generates Prophet forecasts by SKU and Workspace. 
# MAGIC
# MAGIC Recommended -> Run this notebook on a weekly basis to generate forecasts. 
# MAGIC
# MAGIC
# MAGIC This notebook generates and evaluates forecasts. Data is saved to the following tables: 
# MAGIC 1. `input_forecast_dbus` - overwritten each time   
# MAGIC 1. `output_forecasts_dbus` - append only with training data and training uuid  
# MAGIC 1. `output_forecasts_dbus_evaluations` - append only with training data and training uuid  

# COMMAND ----------

# DBTITLE 1,Import libs
from pyspark.sql.functions import *
from libs.dbu_prophet_forecast import DBUProphetForecast
import uuid

# COMMAND ----------

# DBTITLE 1,Create forecast obj
dpf = DBUProphetForecast(forecast_periods=180)

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text('TargetCatalog', 'main')
dbutils.widgets.text('TargetSchema', 'prophet_forecast_schema')
dbutils.widgets.text('ETLCheckpoint', '')
target_catalog = dbutils.widgets.get('TargetCatalog')
target_schema = dbutils.widgets.get('TargetSchema')
etl_checkpoint = dbutils.widgets.get('ETLCheckpoint')


# COMMAND ----------

# DBTITLE 1,Create data objects
# spark.sql(f"create catalog if not exists {target_catalog}")
# spark.sql(f"create schema if not exists {target_catalog}.{target_schema}")
spark.sql(f'use catalog {target_catalog}')
spark.sql(f'use schema {target_schema}')

# COMMAND ----------

### 
## applyInPandas functions require us to declare with a wrapper function here
## Note: look for a way to avoid this
### 
def generate_forecast_udf(history_pd):
    return dpf.generate_forecast( history_pd )
  
def evaluate_forecast_udf(evaluation_pd):
    return dpf.evaluate_forecast( evaluation_pd )


# COMMAND ----------

# Each time we generate forecasts we will archive the billing data 
etl_table_name = f'{target_catalog}.{target_schema}.historical_billing_usage'
dpf.save_billing_data(table_name=etl_table_name, checkpoint_path=etl_checkpoint)

# COMMAND ----------

# DBTITLE 1,Read data from System table
df = dpf.load_data(spark=spark)

# COMMAND ----------

# DBTITLE 1,Group and save dataframe as table 
## the input data we will want to overwrite with each training run 
input_df = dpf.transform_data(df)
(
  input_df
  .write
  .option("mergeSchema", "true")
  .mode('overwrite')
  .saveAsTable(f"{target_catalog}.{target_schema}.input_forecast_dbus")
)

# COMMAND ----------

# DBTITLE 1,Generate Forecasts
results = (
  input_df
    .groupBy('workspace_id','sku')
    .applyInPandas(generate_forecast_udf, schema=dpf.forecast_result_schema)
    .withColumn('training_date', current_timestamp() )
    .withColumn('training_id', lit(str(uuid.uuid4())))
    )

# COMMAND ----------

# DBTITLE 1,Save Results
(
  results.write
  .option("mergeSchema", "true")
  .mode('append')
  .saveAsTable(f"{target_catalog}.{target_schema}.output_forecasts_dbus")
)

# COMMAND ----------

# DBTITLE 1,Evaluate Forecasts
results = (
  spark.read
    .table(f'{target_catalog}.{target_schema}.output_forecasts_dbus')
    .select('training_date', 'training_id', 'workspace_id', 'sku', 'y', 'yhat')
    .groupBy('training_date', 'training_id', 'workspace_id', 'sku')
    .applyInPandas(evaluate_forecast_udf, schema=dpf.eval_schema)
    )


# COMMAND ----------

# DBTITLE 1,Save Evaluation Results
(
  results.write
  .option("mergeSchema", "true")
  .mode('append')
  .saveAsTable(f"{target_catalog}.{target_schema}.output_forecasts_dbus_evaluations")
)
