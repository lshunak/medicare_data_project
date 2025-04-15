WITH source AS (
    SELECT *
    FROM {{ ref('stg_beneficiaries') }}
),

enhanced AS (
    SELECT
        -- Primary Key
        beneficiary_id,
        
        -- Demographics
        birth_date,
        gender,
        DATEDIFF('YEAR', birth_date, CURRENT_DATE()) as age,
        
        -- Status
        CASE 
            WHEN death_date IS NOT NULL THEN 'Deceased'
            ELSE 'Living'
        END as status,
        
        -- Coverage
        CASE 
            WHEN part_a_months > 0 AND part_b_months > 0 THEN 'Full Coverage'
            WHEN part_a_months > 0 THEN 'Part A Only'
            WHEN part_b_months > 0 THEN 'Part B Only'
            ELSE 'No Coverage'
        END as coverage_status,
        
        -- Original values for reference
        part_a_months,
        part_b_months,
        death_date,
        
        -- Metadata
        CURRENT_TIMESTAMP() as _loaded_at,
        '{{ invocation_id }}' as _invocation_id,
        '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as _run_id
    FROM source
)

SELECT * FROM enhanced