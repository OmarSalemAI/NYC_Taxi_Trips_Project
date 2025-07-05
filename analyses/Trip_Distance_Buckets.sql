WITH distance_bucket_cte AS (
    SELECT 
        CASE 
            WHEN trip_distance <= 1 THEN '0-1 mile'
            WHEN trip_distance <= 5 THEN '1-5 miles'
            WHEN trip_distance <= 10 THEN '5-10 miles'
            ELSE '10+ miles'
        END AS distance_bucket,
        COUNT(*) AS trip_count
    FROM {{ ref('fact_trips') }}
    GROUP BY 1
)
SELECT * FROM distance_bucket_cte;