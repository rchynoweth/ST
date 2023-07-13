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
from libs.ddl_helper import DDLHelper
import uuid

# COMMAND ----------

# DBTITLE 1,Create forecast obj
dpf = DBUProphetForecast(forecast_periods=180)

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text('TargetCatalog', 'main')
dbutils.widgets.text('TargetSchema', 'prophet_forecast_schema')
target_catalog = dbutils.widgets.get('TargetCatalog')
target_schema = dbutils.widgets.get('TargetSchema')

# COMMAND ----------

# DBTITLE 1,Create data objects
# spark.sql(f"create catalog if not exists {target_catalog}")
spark.sql(f'use catalog {target_catalog}')
spark.sql(f"create schema if not exists {target_catalog}.{target_schema}")
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create BI Tables and Views 

# COMMAND ----------

## DDL Helper is used to create a cost lookup table
# create obj
ddl_help = DDLHelper(spark=spark)
# create table DDL
ddl_help.create_cost_lookup_table(target_catalog=target_catalog, target_schema=target_schema)
# insert overwrite into tale
ddl_help.insert_into_cost_lookup_table(target_catalog=target_catalog, target_schema=target_schema)

# COMMAND ----------

# Reporting view on top of our raw forecast outputs 
ddl_help.create_granular_forecast_view(target_catalog=target_catalog, target_schema=target_schema)
