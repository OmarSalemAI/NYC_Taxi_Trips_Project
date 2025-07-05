SELECT 
    *,
    trip_distance
FROM {{ ref('fact_trips') }}
WHERE trip_distance < 0