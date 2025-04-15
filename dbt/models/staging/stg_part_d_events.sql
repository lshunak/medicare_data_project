SELECT
    -- Event Identifiers
    $1:PDE_ID::VARCHAR(20) as prescription_event_id,
    $1:BENE_ID::VARCHAR(16) as beneficiary_id,

    -- Prescription Details
    $1:SRVC_DT::DATE as service_date,
    $1:PROD_SRVC_ID::VARCHAR(20) as product_service_id,
    $1:QTY_DSPNSD_NUM::NUMBER(10,2) as quantity_dispensed,
    $1:DAYS_SUPLY_NUM::NUMBER as days_supply,
    
    -- Payment Information
    $1:TOT_RX_CST_AMT::NUMBER(10,2) as total_cost,
    $1:PTD_TOT_AMT::NUMBER(10,2) as part_d_paid_amount,
    $1:PTNT_PAY_AMT::NUMBER(10,2) as patient_paid_amount,
    
    -- Provider Information
    $1:PRSCRBR_ID::VARCHAR(15) as prescriber_id,
    $1:SRVC_PRVDR_ID::VARCHAR(15) as service_provider_id,
    
    -- Drug Information
    $1:BRND_GNRC_CD::VARCHAR(1) as brand_generic_code,
    $1:FRMLRY_ID::VARCHAR(8) as formulary_id,
    
    -- Metadata
    $1:VALID_FROM::TIMESTAMP as valid_from,
    $1:VALID_TO::TIMESTAMP as valid_to,
    CURRENT_TIMESTAMP() as loaded_at

FROM {{ source('medicare_raw', 'PART_D_EVENTS_EXTERNAL') }}