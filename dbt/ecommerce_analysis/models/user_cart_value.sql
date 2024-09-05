WITH user_cart_value AS (
    SELECT
        ip,
        SUM(sales) AS total_sales,
        COUNT(*) AS sessions_count,
        CASE WHEN COUNT(*) > 0 THEN SUM(sales) / COUNT(*) ELSE 0 END AS avg_cart_value
    FROM {{ source('ecommerce_source', 'ecommerce_logs') }}
    GROUP BY ip
)
SELECT
    ip,
    total_sales,
    sessions_count,
    avg_cart_value
FROM user_cart_value
ORDER BY avg_cart_value DESC
LIMIT 500
