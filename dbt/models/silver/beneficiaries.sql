-- models/silver/beneficiaries.sql
{{
    config(
        materialized='table',
        tags=['silver']
    )
}}

SELECT 
    *,
    DATEDIFF('year', birth_date, CURRENT_DATE()) as age,
    CASE 
        WHEN death_date IS NOT NULL THEN TRUE 
        ELSE FALSE 
    END as is_deceased
FROM {{ ref('base_beneficiaries') }}