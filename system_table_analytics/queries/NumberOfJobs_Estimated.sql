SELECT 
DATE_FORMAT(usage_start_time, 'yyyy-MM') as MonthYear,

COUNT(DISTINCT concat(usage_metadata.job_id, usage_metadata.cluster_id)) as WorkspaceCount

FROM system.billing.usage

where usage_metadata.job_id is not null

GROUP BY 1