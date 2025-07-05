WITH payment_counts_cte AS (
    SELECT 
        p.payment_type,
        COUNT(*) AS trip_count
    FROM {{ ref('fact_trips') }} f
    JOIN {{ ref('dim_payment_type') }} p ON f.payment_type_id = p.payment_type_id
    GROUP BY p.payment_type
),
ranked AS (
    SELECT *, RANK() OVER (ORDER BY trip_count DESC) AS rnk
    FROM payment_counts_cte
)
SELECT payment_type, trip_count
FROM ranked
WHERE rnk = 1;