{{ config(
    materialized='incremental',
    unique_key='DATE'
) }}

WITH source_data AS (
    SELECT 
        DATE,
        REGION,
        PRODUCT,
        AMOUNT,
        PROCESSED_AT
    FROM {{ source('snowflake_raw', 'SALES_CLEAN') }}

    -- THE SENIOR LEVEL LOGIC:
    -- If this is an incremental run, only fetch data that is new or changed
    {% if is_incremental() %}
      -- We look back 3 days just in case data arrived late
      WHERE PROCESSED_AT >= (SELECT DATEADD('day', -3, MAX(PROCESSED_AT)) FROM {{ this }})
    {% endif %}
)

SELECT 
    DATE,
    REGION,
    SUM(AMOUNT) as TOTAL_REVENUE,
    COUNT(*) as TRANSACTION_COUNT,
    MAX(PROCESSED_AT) as PROCESSED_AT -- Keep track of when we last saw data for this day
FROM source_data
GROUP BY 1, 2