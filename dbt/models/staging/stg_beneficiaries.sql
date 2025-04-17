
SELECT
    -- Core Identifiers
    $1:BENE_ID::VARCHAR(20) as beneficiary_id,
    
    -- Demographics
    $1:BENE_BIRTH_DT::DATE as birth_date,
    $1:BENE_DEATH_DT::DATE as death_date,
    $1:SEX_IDENT_CD::VARCHAR(1) as gender,
    $1:BENE_RACE_CD::VARCHAR(1) as race_code,
    $1:BENE_CNTY_CD::VARCHAR(3) as county_code,
    $1:BENE_STATE_CD::VARCHAR(2) as state_code,
    $1:BENE_ZIP_CD::VARCHAR(9) as zip_code,

    -- Coverage Information
    $1:BENE_ENROLLMT_REF_YR::NUMBER as enrollment_year,
    $1:BENE_PTA_STUS_CD::VARCHAR(2) as part_a_status,
    $1:BENE_PTB_STUS_CD::VARCHAR(2) as part_b_status,
    $1:BENE_HI_CVRAGE_TOT_MONS::NUMBER as part_a_months,
    $1:BENE_SMI_CVRAGE_TOT_MONS::NUMBER as part_b_months,
    $1:BENE_HMO_CVRAGE_TOT_MONS::NUMBER as hmo_months,
    $1:PTD_PLAN_CVRG_MONS::NUMBER as part_d_months,

    -- Medicare Status
    $1:BENE_MDCR_STATUS_CD::VARCHAR(2) as medicare_status,
    $1:BENE_DUAL_STUS_CD::VARCHAR(2) as dual_status,

    -- Metadata
    $1:VALID_FROM::TIMESTAMP as valid_from,
    $1:VALID_TO::TIMESTAMP as valid_to,
    CURRENT_TIMESTAMP() as loaded_at

FROM {{ source('medicare_raw', 'BENEFICIARY_EXTERNAL') }}