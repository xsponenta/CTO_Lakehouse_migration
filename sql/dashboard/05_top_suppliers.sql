SELECT rank, supplier_name, nation, region, revenue, share_pct
FROM agg_supplier_revenue_yearly
WHERE year = (SELECT max(period_label) FROM agg_revenue_year WHERE is_complete)
  AND rank <= 10
ORDER BY rank
