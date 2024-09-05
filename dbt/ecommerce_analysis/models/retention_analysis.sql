WITH user_activity AS (
    SELECT
        ip,
        COUNT(DISTINCT DATE_TRUNC('day', accessed_date)) AS active_days,
        COUNT(*) AS total_sessions,
        MAX(accessed_date) AS last_session_date
    FROM {{ source('ecommerce_source', 'ecommerce_logs') }}
    GROUP BY ip
)
SELECT
    ip,
    active_days,
    total_sessions,
    last_session_date
FROM user_activity
ORDER BY active_days DESC
LIMIT 500
