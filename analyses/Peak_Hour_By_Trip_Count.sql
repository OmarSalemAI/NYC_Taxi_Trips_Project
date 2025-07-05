WITH peak_hour_cte AS (
    SELECT 
        t.hour,
        COUNT(*) AS trip_count
    FROM {{ ref('fact_trips') }} f
    JOIN {{ ref('dim_time') }} t ON f.pickup_time_id = t.time_id
    GROUP BY t.hour
)
SELECT hour, trip_count
FROM peak_hour_cte
ORDER BY trip_count DESC
LIMIT 1;