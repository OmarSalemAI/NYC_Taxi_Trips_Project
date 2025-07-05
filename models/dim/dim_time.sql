{{
    config(
        materialized='table',
        unique_key='time_id'
    )
}}

WITH time_range AS (
    -- Generate 86400 seconds in a day
    SELECT 
        DATEADD(second, seq4(), TO_TIMESTAMP_NTZ('2000-01-01 00:00:00')) AS time_val
    FROM TABLE(GENERATOR(ROWCOUNT => 86400))
),

dim_time AS (
    SELECT 
        TO_CHAR(time_val, 'HH24MISS') AS time_id,
        EXTRACT(HOUR FROM time_val) AS hour,
        EXTRACT(MINUTE FROM time_val) AS minute,
        EXTRACT(SECOND FROM time_val) AS second
    FROM time_range
)

SELECT *
FROM dim_time
ORDER BY time_id
