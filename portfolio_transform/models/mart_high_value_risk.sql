{{ config(materialized='table') }}

WITH churn_data AS (
    SELECT 
        CUSTOMER_ID,
        MONTHLY_BILL,
        CONTRACT_TYPE,
        CS_CALLS
    FROM {{ source('snowflake_raw', 'CUSTOMER_CHURN_DATA') }}
),

reviews AS (
    SELECT 
        PRODUCT_NAME,
        SENTIMENT_LABEL,
        SUMMARY
    FROM {{ source('snowflake_raw', 'REVIEWS_ANALYZED') }}
)

-- Logic: Find customers paying > $100 who have made support calls
SELECT 
    c.CUSTOMER_ID,
    c.MONTHLY_BILL,
    c.CS_CALLS,
    CASE 
        WHEN c.CS_CALLS > 3 THEN 'High Risk' 
        ELSE 'Normal' 
    END as RISK_CATEGORY
FROM churn_data c
WHERE c.MONTHLY_BILL > 100