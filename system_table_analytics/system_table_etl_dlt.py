# Databricks notebook source
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC # System Table ETL 
# MAGIC
# MAGIC System tables provided by Databricks have a 12 month retention period. This is a DLT pipeline that can be executed daily to archive system data to another schema. 
# MAGIC
# MAGIC To use this pipeline, simply add the tables to the `tables` python list object and it will incrementally load the table. 
# MAGIC
# MAGIC Create a DLT pipeline by following these [instructions](https://docs.databricks.com/delta-live-tables/tutorial-pipelines.html).

# COMMAND ----------

tables = [
  '`system.billing.usage`'
  , '`system.access.audit`'
]

# COMMAND ----------

### 
# This creates append only tables for our bronze sources
# we can do further modeling in silver/gold layers 
###
def generate_tables(table):  
  @dlt.table( name=table.replace('.', '_'), comment=f"BRONZE: {table.replace('.', '_')}" )  
  def create_table():    
    return (spark.readStream
        .format("delta")
        .table(table)
    )  


# COMMAND ----------

# for each table we pass it into the above function
for t in tables:  
  generate_tables(t)

# COMMAND ----------

display(spark.sql('select * from system.billing.usage'))

# COMMAND ----------


