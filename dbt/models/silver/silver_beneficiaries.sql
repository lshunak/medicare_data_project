
WITH source AS (
    SELECT *
    FROM {{ ref('stg_beneficiaries') }}
),

latest_records AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY beneficiary_id 
               ORDER BY loaded_at DESC
           ) as row_number
    FROM source
),

current_beneficiaries AS (
    SELECT 
        -- Core Identifiers
        beneficiary_id,
        
        -- Demographics (cleaned)
        birth_date,
        death_date,
        CASE 
            WHEN gender IN ('1', '2') THEN gender
            ELSE 'U' 
        END as gender,
        
        CASE 
            WHEN race_code IN ('1','2','3','4','5') THEN race_code
            ELSE 'U' 
        END as race_code,
        
        -- Location (cleaned)
        NULLIF(state_code, '99') as state_code,
        NULLIF(county_code, '999') as county_code,
        CASE 
            WHEN LENGTH(TRIM(zip_code)) >= 5 THEN LEFT(TRIM(zip_code), 5)
            ELSE NULL 
        END as zip_code,
        
        -- Medicare Coverage
        enrollment_year,
        part_a_status,
        part_b_status,
        NULLIF(part_a_months, 0) as part_a_months,
        NULLIF(part_b_months, 0) as part_b_months,
        NULLIF(hmo_months, 0) as hmo_months,
        NULLIF(part_d_months, 0) as part_d_months,
        medicare_status,
        dual_status,
        
        -- Derived Fields
        DATEDIFF('YEAR', birth_date, COALESCE(death_date, CURRENT_DATE())) as age,
        
        CASE 
            WHEN death_date IS NOT NULL THEN TRUE
            ELSE FALSE 
        END as is_deceased,
        
        CASE
            WHEN part_a_status IN ('10', '11') 
             AND part_b_status IN ('10', '11') THEN TRUE
            ELSE FALSE
        END as has_full_coverage,
        
        CASE
            WHEN part_d_months > 0 THEN TRUE
            ELSE FALSE
        END as has_drug_coverage,
        
        -- Record tracking
        loaded_at as valid_from,
        CASE 
            WHEN row_number = 1 THEN NULL
            ELSE loaded_at 
        END as valid_to,
        TRUE as is_current,
        
        -- Metadata
        '2025-04-17 08:39:50'::timestamp as dbt_updated_at,
        'lshunak' as dbt_updated_by
        
    FROM latest_records
    WHERE row_number = 1  -- Only keep current records
)

SELECT * FROM current_beneficiaries