WITH daily_summary_cte AS (
    SELECT 
        d.full_date,
        SUM(f.total_amt) AS daily_revenue,
        COUNT(*) AS daily_trips
    FROM {{ ref('fact_trips') }} f
    JOIN {{ ref('dim_date') }} d ON f.pickup_date_id = d.date_id
    GROUP BY d.full_date
),
daily_averages AS (
    SELECT 
        ROUND(AVG(daily_revenue), 2) AS avg_daily_revenue,
        ROUND(AVG(daily_trips), 2) AS avg_daily_trips
    FROM daily_summary_cte
)
SELECT * FROM daily_averages;
