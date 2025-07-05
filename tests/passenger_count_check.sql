SELECT 
    *,
    passenger_count
FROM {{ ref('fact_trips') }}
WHERE passenger_count NOT BETWEEN 1 AND 10 