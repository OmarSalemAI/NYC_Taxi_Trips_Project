WITH dim_zone AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY zone, borough ) AS zone_id,
        zone,
        borough
    FROM (
        SELECT DISTINCT 
                initcap(trim(start_zone)) AS zone,
                initcap(trim(start_borough)) AS borough
        FROM {{ ref('stg_trips') }}
        WHERE zone IS NOT NULL AND borough IS NOT NULL
        GROUP BY zone, borough

    union 

        SELECT DISTINCT 
                initcap(trim(end_zone)) AS zone,
                initcap(trim(end_borough)) AS borough
        FROM {{ ref('stg_trips') }}
        WHERE zone IS NOT NULL AND borough IS NOT NULL
        GROUP BY zone, borough
    ) all_zones
)

SELECT zone_id, zone, borough
FROM dim_zone
ORDER BY zone_id