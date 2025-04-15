SELECT
    -- Claim Identifiers
    $1:CLM_ID::VARCHAR(20) as claim_id,
    $1:BENE_ID::VARCHAR(16) as beneficiary_id,

    -- Claim Details
    $1:CLM_FROM_DT::DATE as claim_start_date,
    $1:CLM_THRU_DT::DATE as claim_end_date,
    $1:CLM_PMT_AMT::NUMBER(10,2) as claim_payment_amount,
    $1:NCH_PRMRY_PYR_CLM_PD_AMT::NUMBER(10,2) as primary_payer_paid_amount,
    $1:NCH_CLM_TYPE_CD::VARCHAR(2) as claim_type_cd,  
    
    -- Provider Information
    $1:PRVDR_NUM::VARCHAR(10) as provider_number,
    $1:AT_PHYSN_NPI::VARCHAR(10) as attending_physician_npi,
    $1:OP_PHYSN_NPI::VARCHAR(10) as operating_physician_npi,
    
    -- Diagnosis Codes
    $1:ICD_DGNS_CD1::VARCHAR(7) as diagnosis_code_1,
    $1:ICD_DGNS_CD2::VARCHAR(7) as diagnosis_code_2,
    $1:ICD_DGNS_CD3::VARCHAR(7) as diagnosis_code_3,
    
    -- Procedure Codes
    $1:ICD_PRCDR_CD1::VARCHAR(7) as procedure_code_1,
    $1:ICD_PRCDR_CD2::VARCHAR(7) as procedure_code_2,
    
    -- Payment Information
    $1:CLM_TOT_CHRG_AMT::NUMBER(10,2) as total_charge_amount,
    $1:NCH_BENE_PTB_DDCTBL_AMT::NUMBER(10,2) as deductible_amount,
    $1:NCH_BENE_PTB_COINSRNC_AMT::NUMBER(10,2) as coinsurance_amount,
    
    -- Metadata
    $1:VALID_FROM::TIMESTAMP as valid_from,
    $1:VALID_TO::TIMESTAMP as valid_to,
    CURRENT_TIMESTAMP() as loaded_at

FROM {{ source('medicare_raw', 'CLAIMS_EXTERNAL') }}