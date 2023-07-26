
with raw_data as (
select 
  a.usage_date
  , a.workspace_id
  , a.sku_name
  , a.usage_unit
  , a.usage_quantity as dbus
  , b.list_price
  , a.usage_quantity*b.list_price as ApproxListCost

from system.billing.usage a
  inner join main.prophet_forecast_schema.sku_cost_lookup b
)

select 
DATE_FORMAT(usage_date, 'yyyy-MM') as MonthYear,
sum(ApproxListCost) as ApproxListCost

from raw_data 

group by 1
