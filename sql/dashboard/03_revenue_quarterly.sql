SELECT period_label AS quarter, is_complete, revenue, change_pct
FROM agg_revenue_quarter
ORDER BY period_start
