WITH suspicious_trips_cte AS (
    SELECT COUNT(*) AS suspicious_trip_count
    FROM {{ ref('fact_trips') }}
    WHERE trip_distance = 0 OR fare_amt = 0 OR total_amt = 0
)
SELECT * FROM suspicious_trips_cte;