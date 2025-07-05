WITH dim_payment_type AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY payment_type) AS payment_type_id,
        payment_type
    FROM {{ ref('stg_trips') }}
    WHERE payment_type IS NOT NULL
    GROUP BY payment_type
)
SELECT * FROM dim_payment_type