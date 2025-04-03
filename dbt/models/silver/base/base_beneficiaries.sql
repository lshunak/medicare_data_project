{{
    config(
        materialized='ephemeral',
        tags=['base', 'silver', 'beneficiaries']
    )
}}

SELECT
    bene_id,
    birth_date,
    death_date,
    gender_cd,
    race_cd,
    state_cd,
    county_cd,
    zip_cd,
    medicare_status_cd,
    dual_status_cd,
    CURRENT_TIMESTAMP() as etl_updated_at,
    CURRENT_USER() as etl_updated_by
FROM {{ source('medicare_raw', 'beneficiaries') }}