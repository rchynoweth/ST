SELECT 
DATE_FORMAT(event_date, 'yyyy-MM') as MonthYear,
COUNT(DISTINCT workspace_id) as WorkspaceCount

FROM system.access.audit

GROUP BY 1
