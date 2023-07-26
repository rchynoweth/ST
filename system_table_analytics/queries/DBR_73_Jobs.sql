select 
workspace_id, 
event_date, 
request_params.cluster_name,
request_params.custom_tags,
get_json_object(request_params.custom_tags,'$.JobId' ) as jobId,
get_json_object(request_params.azure_attributes, '$.availability'),
request_params.spark_version,
request_params

from system.access.audit


where service_name = 'clusters' and action_name = 'create'

and contains(request_params.spark_version, 'dlt') = False

and contains(request_params.spark_version, '7.3')

and get_json_object(request_params.custom_tags,'$.JobId' ) is not null