SELECT month, top10_supplier_share_pct
FROM kpi_company_health
WHERE revenue_complete
ORDER BY month
