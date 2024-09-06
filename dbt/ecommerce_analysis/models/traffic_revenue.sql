WITH traffic_revenue AS (
    SELECT
        accessed_from,
        SUM(sales) AS total_revenue,
        COUNT(DISTINCT ip) AS unique_visitors,
        SUM(returned_amount) AS total_returns
    FROM {{ source('ecommerce_source', 'ecommerce_logs') }}
    GROUP BY accessed_from
)
SELECT
    accessed_from,
    total_revenue,
    unique_visitors,
    total_returns
FROM traffic_revenue
ORDER BY total_revenue DESC
LIMIT 2000
