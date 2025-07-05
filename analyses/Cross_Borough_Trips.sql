WITH cross_borough_cte AS (
    SELECT COUNT(*) AS cross_borough_trips
    FROM {{ ref('fact_trips') }} f
    JOIN {{ ref('dim_zone') }} pz ON f.pickup_zone_id = pz.zone_id
    JOIN {{ ref('dim_zone') }} dz ON f.dropoff_zone_id = dz.zone_id
    WHERE pz.borough IS NOT NULL 
      AND dz.borough IS NOT NULL
      AND pz.borough <> dz.borough
)
SELECT * FROM cross_borough_cte;