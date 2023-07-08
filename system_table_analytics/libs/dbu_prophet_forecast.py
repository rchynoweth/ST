# Databricks notebook source
from prophet import Prophet
from pyspark.sql.functions import *
from pyspark.sql.types import * 
import pandas as pd
from sklearn.metrics import mean_squared_error, mean_absolute_error
from math import sqrt



class DBUProphetForecast():
  """
  Class for DBU Forecasting
  """
  
  def __init__(self, forecast_periods=7, interval_width=0.85, forecast_frequency='d', include_history=True):
    """
    Initilization function

    
    :param forecast_periods: Periods to forecast. Default is 7. 
    :param interval_width: confidence level of min/max thresholds. Default is 0.85
    :param forecast_frequency: frequency of the ds column. Default is daily i.e. 'd'
    :param include_history: whether or not to include history in the output dataframe. Default is True. 
    """
    self.forecast_periods=forecast_periods
    self.forecast_frequency=forecast_frequency
    self.include_history=include_history
    self.interval_width=interval_width
    
    
    # Training output schema 
    self.forecast_result_schema = StructType([
      StructField('ds',DateType()),
      StructField('workspace_id', StringType()),
      StructField('sku',StringType()),
      StructField('y',FloatType()),
      StructField('yhat',FloatType()),
      StructField('yhat_upper',FloatType()),
      StructField('yhat_lower',FloatType())
      ])
    
    # Evaluation output schema 
    self.eval_schema =StructType([
      StructField('training_date', TimestampType()),
      StructField('training_id', StringType()),
      StructField('workspace_id', StringType()),
      StructField('sku', StringType()),
      StructField('mae', FloatType()),
      StructField('mse', FloatType()),
      StructField('rmse', FloatType())
      ])
    

  def load_data(self, spark):
    """
    Load data from system.billing.usage
    """
    return (
      spark.sql("""
                select workspace_id
                , usage_date as created_on
                , sku_name as sku
                , usage_quantity as dbus

                from system.billing.usage

                where usage_unit = 'DBU'
                """)
      )


  def transform_data(self, df):
    """
    Transforms data with little transforms. Keeping the system SKU as is very better cost estimates. 
    """
    df = df.select(col('created_on').alias('ds'), 
                   col('sku'), 
                   col('workspace_id').cast("string"), 
                   col('dbus').cast('double')
                  )
      
    group_df = (
      df
      .groupBy(col("ds"), col('sku'), col('workspace_id'))
      .agg(sum('dbus').alias("y"))
      )
      
    # filter out sku/workspaces with not enough data
    # prophet requires at least 2 rows, we will arbitrarily use 10 rows as min
    out = group_df.groupBy("sku", "workspace_id").count().filter("count > 10").join(group_df, on=["sku", "workspace_id"], how="inner").drop('count')
      
    return out 


  def generate_forecast(self, history_pd):
    """
    Function to generate forecasts 

    NOTE: Pandas UDFs inside classes must be static 
    """
    # remove missing values (more likely at day-store-item level)
    history_pd = history_pd.dropna()
    
    # train and configure the model
    model = Prophet( interval_width=self.interval_width )
    model.fit( history_pd )

    # make predictions
    future_pd = model.make_future_dataframe(
      periods=self.forecast_periods, 
      freq=self.forecast_frequency, 
      include_history=self.include_history
      )
    forecast_pd = model.predict( future_pd )  
    
    # ASSEMBLE EXPECTED RESULT SET
    # --------------------------------------
    # get relevant fields from forecast
    f_pd = forecast_pd[ ['ds','yhat', 'yhat_upper', 'yhat_lower'] ].set_index('ds')
    
    # get relevant fields from history
    h_pd = history_pd[['ds','workspace_id','sku','y']].set_index('ds')
    
    # join history and forecast
    results_pd = f_pd.join( h_pd, how='left' )
    results_pd.reset_index(level=0, inplace=True)
    
    # get sku & workspace id from incoming data set
    results_pd['sku'] = history_pd['sku'].iloc[0]
    results_pd['workspace_id'] = history_pd['workspace_id'].iloc[0]

    return results_pd[ ['ds', 'workspace_id', 'sku', 'y', 'yhat', 'yhat_upper', 'yhat_lower'] ]  

  def evaluate_forecast(self, evaluation_pd):
    """
    Forecast evaluation function. Generates MAE, RMSE, MSE metrics. 

    NOTE: Pandas UDFs inside classes must be static 
    """
    evaluation_pd = evaluation_pd[evaluation_pd['y'].notnull()]
    # get sku in incoming data set
    training_date = evaluation_pd['training_date'].iloc[0]
    training_id = evaluation_pd['training_id'].iloc[0]
    sku = evaluation_pd['sku'].iloc[0]
    workspace_id = evaluation_pd['workspace_id'].iloc[0]
    
    # calulate evaluation metrics
    mae = mean_absolute_error( evaluation_pd['y'], evaluation_pd['yhat'] )
    mse = mean_squared_error( evaluation_pd['y'], evaluation_pd['yhat'] )
    rmse = sqrt( mse )
    
    # assemble result set
    results = {'training_date':[training_date], 'training_id': [training_id], 'workspace_id':[workspace_id], 'sku':[sku], 'mae':[mae], 'mse':[mse], 'rmse':[rmse]}
    return pd.DataFrame.from_dict( results )
  


