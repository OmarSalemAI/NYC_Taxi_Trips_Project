WITH revenue_per_mile AS (
    SELECT
        trip_distance,
        total_amt,
        total_amt / trip_distance AS revenue_per_mile
    FROM {{ ref('fact_trips') }}
)

SELECT
    ROUND(AVG(revenue_per_mile), 2) AS avg_revenue_per_mile
FROM revenue_per_mile