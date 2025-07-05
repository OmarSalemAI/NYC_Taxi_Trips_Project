
{{
    config(
        materialized='incremental',
        unique_key='trip_id'
    )}}

WITH stg_trips AS (
    SELECT *
    FROM NYC_TRIPS.raw.raw_trips
)

SELECT 
    {{ dbt_utils.generate_surrogate_key([
        'vendor_name',
        'trip_pickup_datetime',
        'trip_dropoff_datetime',
        'passenger_count',
        'trip_distance',
        'start_lat',
        'start_lon',
        'rate_code',
        'store_and_forward',
        'end_lat',
        'end_lon',
        'fare_amt'
    ]) }} as trip_id,

    trim(vendor_name) as vendor_name,
    TO_TIMESTAMP_NTZ(trip_pickup_datetime) as trip_pickup_datetime,
    TO_TIMESTAMP_NTZ(trip_dropoff_datetime) as trip_dropoff_datetime,
    passenger_count,
    trip_distance,
    start_lat,
    start_lon,
    CASE
        WHEN fare_amt BETWEEN 50 AND 60 AND tolls_amt > 0 AND surcharge >= 0.5 THEN 2
        WHEN fare_amt > 60 AND tolls_amt > 0 THEN 3
        WHEN fare_amt > 80 AND tolls_amt = 0 THEN 4
        WHEN fare_amt < 30 AND tip_amt = 0 AND surcharge > 0 THEN 5
        WHEN fare_amt BETWEEN 15 AND 25 AND passenger_count > 1 THEN 6
        ELSE 1
    END AS rate_code,
    CASE 
        WHEN store_and_forward IN (0, 0.0) THEN 'NO'
        WHEN store_and_forward IN (1, 1.0) THEN 'YES'
        ELSE 'UNKNOWN' 
    END as store_and_forward,
    end_lat,
    end_lon,
    initcap(trim(payment_type)) as payment_type,
    fare_amt,
    surcharge,
    tip_amt,
    tolls_amt,
    total_amt,
    start_zone,
    start_borough,
    end_zone,
    end_borough
FROM stg_trips

WHERE 
    passenger_count BETWEEN 1 AND 10 
    AND trip_dropoff_datetime > trip_pickup_datetime
    AND trip_distance > 0
    AND total_amt > 0
    AND tolls_amt >= 0
    AND tip_amt >= 0
    AND surcharge >= 0
    AND vendor_name IS NOT NULL
    AND trip_pickup_datetime IS NOT NULL
    AND trip_dropoff_datetime IS NOT NULL
    AND fare_amt IS NOT NULL

{% if is_incremental() %}
AND trip_pickup_datetime > (SELECT MAX(trip_pickup_datetime) FROM {{ this }})
{% endif %}
