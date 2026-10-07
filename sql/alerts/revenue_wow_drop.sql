-- alert condition: change_pct <= -20
SELECT
  period_start  AS week_start,
  period_end    AS week_end,
  prev_revenue,
  revenue,
  change_pct
FROM agg_revenue_week
WHERE is_complete
  AND change_pct IS NOT NULL
ORDER BY period_start DESC
LIMIT 1
