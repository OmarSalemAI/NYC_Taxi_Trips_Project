{{
    config(
        materialized='incremental',
        unique_key='date_id'
    )
}}

WITH all_dates AS (
    SELECT TO_DATE(trip_pickup_datetime) AS date
    FROM {{ ref('stg_trips') }}

    UNION

    SELECT TO_DATE(trip_dropoff_datetime) AS date
    FROM {{ ref('stg_trips') }}
),

distinct_dates AS (
    SELECT DISTINCT
        TO_CHAR(date, 'YYYYMMDD') AS date_id,
        date
    FROM all_dates

    {% if is_incremental() %}
    -- Only include new dates that aren't already in the target table
    WHERE TO_CHAR(date, 'YYYYMMDD') NOT IN (
        SELECT date_id FROM {{ this }}
    )
    {% endif %}
)

SELECT
    dd.date_id,
    dd.date,
    EXTRACT(YEAR FROM dd.date) AS year,
    EXTRACT(MONTH FROM dd.date) AS month,
    EXTRACT(DAY FROM dd.date) AS day
FROM distinct_dates dd
