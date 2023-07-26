SELECT workspace_id, MIN(usage_date) as min_date
FROM system.billing.usage
WHERE usage_quantity > 0
GROUP BY workspace_id