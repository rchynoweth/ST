SELECT 
DATE_FORMAT(event_date, 'yyyy-MM') as MonthYear,

COUNT(DISTINCT user_identity.email) as UserCount

FROM system.access.audit

GROUP BY 1
