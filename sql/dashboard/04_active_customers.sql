SELECT as_of, active_customers
FROM agg_active_customers_monthly
WHERE is_complete
ORDER BY as_of
