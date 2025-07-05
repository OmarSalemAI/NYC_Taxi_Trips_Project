WITH dim_store_and_forward AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY transmission_flag) AS transmission_id,
        transmission_flag
    FROM (
        SELECT store_and_forward AS transmission_flag
        FROM {{ ref('stg_trips') }}
        WHERE transmission_flag IS NOT NULL
        GROUP BY transmission_flag
    )
    
)

SELECT transmission_id, transmission_flag
FROM dim_store_and_forward
ORDER BY transmission_id