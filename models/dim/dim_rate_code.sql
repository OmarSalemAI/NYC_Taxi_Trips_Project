WITH dim_rate_code AS (
    SELECT
        rate_code,
        CASE
            WHEN rate_code = '1' THEN 'Standard rate'
            WHEN rate_code = '2' THEN 'JFK Airport'
            WHEN rate_code = '3' THEN 'Newark Airport'
            WHEN rate_code = '4' THEN 'Nassau or Westchester'
            WHEN rate_code = '5' THEN 'Negotiated fare'
            WHEN rate_code = '6' THEN 'Group ride'
            ELSE 'Unknown'
        END AS rate_code_description
    FROM {{ ref('stg_trips') }}
    WHERE rate_code IS NOT NULL
    GROUP BY rate_code
)
SELECT * FROM dim_rate_code
ORDER BY rate_code