SELECT
  month_label,
  revenue,
  revenue_yoy_pct,
  active_customers_12m,
  top10_supplier_share_pct
FROM kpi_company_health
WHERE revenue_complete AND active_window_complete
ORDER BY month DESC
LIMIT 1
