SELECT period_start AS week_start, revenue, change_pct
FROM agg_revenue_week
WHERE is_complete
ORDER BY week_start
