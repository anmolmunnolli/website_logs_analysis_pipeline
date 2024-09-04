WITH sessions AS (
    SELECT
        ip,
        SUM(duration_secs) AS total_session_duration,
        COUNT(*) AS sessions_count,
        SUM(sales) AS total_sales,
        SUM(returned_amount) AS total_returned,
        MAX(network_protocol) AS most_common_protocol
    FROM {{ source('ecommerce_source', 'ecommerce_logs') }}
    GROUP BY ip
)
SELECT
    ip,
    total_session_duration,
    sessions_count,
    total_sales,
    total_returned,
    most_common_protocol
FROM sessions
LIMIT 5
