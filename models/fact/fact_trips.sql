{{ config(
    materialized='incremental',
    unique_key='trip_id'
) }}

WITH trips_base AS (
    SELECT *,
        TO_CHAR(trip_pickup_datetime, 'YYYYMMDDHH24MISS') AS pickup_datetime_id
    FROM {{ ref('stg_trips') }}
),

-- This JOIN filters trips that are *not already in* the fact table (only when incremental)
new_trips AS (
    SELECT b.*
    FROM trips_base b
    {% if is_incremental() %}
    LEFT JOIN {{ this }} existing
        ON b.trip_id = existing.trip_id
    WHERE existing.trip_id IS NULL
    {% endif %}
),

-- Dim references
pickup_date AS ( SELECT * FROM {{ ref('dim_date') }} ),
dropoff_date AS ( SELECT * FROM {{ ref('dim_date') }} ),
pickup_time AS ( SELECT * FROM {{ ref('dim_time') }} ),
dropoff_time AS ( SELECT * FROM {{ ref('dim_time') }} ),
pickup_zone AS ( SELECT * FROM {{ ref('dim_zone') }} ),
dropoff_zone AS ( SELECT * FROM {{ ref('dim_zone') }} ),
payment_type AS ( SELECT * FROM {{ ref('dim_payment_type') }} ),
rate_code AS ( SELECT * FROM {{ ref('dim_rate_code') }} ),
vendor AS ( SELECT * FROM {{ ref('dim_vendor') }} ),
store_and_forward AS ( SELECT * FROM {{ ref('dim_store_and_forward') }} )

-- Final SELECT
SELECT 
    t.trip_id,
    v.vendor_id,
    p.date_id AS pickup_date_id,
    d.date_id AS dropoff_date_id,
    pt.time_id AS pickup_time_id,
    dt.time_id AS dropoff_time_id,
    pp.zone_id AS pickup_zone_id,
    dd.zone_id AS dropoff_zone_id,
    t.passenger_count,
    t.trip_distance,
    t.start_lat,
    t.start_lon,
    t.end_lat,
    t.end_lon,
    py.payment_type_id,
    t.rate_code,
    s.transmission_id,
    t.fare_amt,
    t.surcharge,
    t.tip_amt,
    t.tolls_amt,
    t.total_amt

FROM new_trips t
LEFT JOIN vendor v ON t.vendor_name = v.vendor_name
LEFT JOIN pickup_date p ON TO_DATE(t.trip_pickup_datetime) = p.date
LEFT JOIN dropoff_date d ON TO_DATE(t.trip_dropoff_datetime) = d.date
LEFT JOIN pickup_time pt ON TO_CHAR(t.trip_pickup_datetime, 'HH24MISS') = pt.time_id
LEFT JOIN dropoff_time dt ON TO_CHAR(t.trip_dropoff_datetime, 'HH24MISS') = dt.time_id
LEFT JOIN pickup_zone pp ON LOWER(TRIM(t.start_zone)) = LOWER(TRIM(pp.zone))
LEFT JOIN dropoff_zone dd ON LOWER(TRIM(t.end_zone)) = LOWER(TRIM(dd.zone))
LEFT JOIN payment_type py ON t.payment_type = py.payment_type
LEFT JOIN store_and_forward s ON t.store_and_forward = s.transmission_flag
