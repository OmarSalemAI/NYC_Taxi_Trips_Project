
WITH dim_vendor AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY initcap(trim(vendor_name))) AS vendor_id,
        trim(vendor_name) AS vendor_name
    FROM {{ ref('stg_trips') }}
    WHERE vendor_name IS NOT NULL
    GROUP BY vendor_name
)

SELECT vendor_id, vendor_name
FROM dim_vendor
ORDER BY vendor_id