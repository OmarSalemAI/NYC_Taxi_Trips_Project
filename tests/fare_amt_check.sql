SELECT 
    trip_id,
    fare_amt
FROM {{ ref('fact_trips') }}
WHERE fare_amt < 0