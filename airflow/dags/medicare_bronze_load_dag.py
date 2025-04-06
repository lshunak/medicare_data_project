from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta

default_args = {
    'owner': 'lshunak',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 6, 11, 45, 36),  # Updated timestamp
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # Increased retries
    'retry_delay': timedelta(minutes=5),
}

# SQL commands for creating stage and file format
create_file_format = """
CREATE OR REPLACE FILE FORMAT MEDICARE_BRONZE_DB.PUBLIC.PARQUET_FORMAT
    TYPE = 'PARQUET'
    BINARY_AS_TEXT = FALSE
    TRIM_SPACE = TRUE;
"""

create_external_stage = """
CREATE OR REPLACE STAGE MEDICARE_BRONZE_DB.PUBLIC.MEDICARE_STAGE
    URL = 's3://lshunak-cms-bucket/processed/'
    CREDENTIALS = (AWS_KEY_ID = '{{ conn.aws_default.login }}'
                  AWS_SECRET_KEY = '{{ conn.aws_default.password }}')
    FILE_FORMAT = MEDICARE_BRONZE_DB.PUBLIC.PARQUET_FORMAT;
"""

create_external_tables = """
-- Beneficiary external table
CREATE OR REPLACE EXTERNAL TABLE MEDICARE_BRONZE_DB.PUBLIC.BENEFICIARY_EXTERNAL
WITH LOCATION = @MEDICARE_STAGE/beneficiary/
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*[.]parquet';

-- Claims external table (unified for all claim types)
CREATE OR REPLACE EXTERNAL TABLE MEDICARE_BRONZE_DB.PUBLIC.CLAIMS_EXTERNAL
WITH LOCATION = @MEDICARE_STAGE/claims/
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*/.*[.]parquet';

-- Part D Events external table
CREATE OR REPLACE EXTERNAL TABLE MEDICARE_BRONZE_DB.PUBLIC.PART_D_EVENTS_EXTERNAL
WITH LOCATION = @MEDICARE_STAGE/part_d/
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*[.]parquet';
"""

create_materialized_tables = """
-- Beneficiary table
CREATE OR REPLACE TABLE MEDICARE_BRONZE_DB.PUBLIC.BENEFICIARY AS
SELECT 
    *,
    METADATA$FILENAME as SOURCE_FILE,
    CURRENT_TIMESTAMP() as LOAD_TIMESTAMP
FROM MEDICARE_BRONZE_DB.PUBLIC.BENEFICIARY_EXTERNAL;

-- Claims table (unified)
CREATE OR REPLACE TABLE MEDICARE_BRONZE_DB.PUBLIC.CLAIMS AS
SELECT 
    *,
    CASE 
        WHEN METADATA$FILENAME LIKE '%/carrier/%' THEN 'CARRIER'
        WHEN METADATA$FILENAME LIKE '%/dme/%' THEN 'DME'
        WHEN METADATA$FILENAME LIKE '%/hha/%' THEN 'HHA'
        WHEN METADATA$FILENAME LIKE '%/hospice/%' THEN 'HOSPICE'
        WHEN METADATA$FILENAME LIKE '%/inpatient/%' THEN 'INPATIENT'
        WHEN METADATA$FILENAME LIKE '%/outpatient/%' THEN 'OUTPATIENT'
        WHEN METADATA$FILENAME LIKE '%/snf/%' THEN 'SNF'
    END as SOURCE_FILE_CLAIM_TYPE,
    METADATA$FILENAME as SOURCE_FILE,
    CURRENT_TIMESTAMP() as LOAD_TIMESTAMP
FROM MEDICARE_BRONZE_DB.PUBLIC.CLAIMS_EXTERNAL;

-- Part D Events table
CREATE OR REPLACE TABLE MEDICARE_BRONZE_DB.PUBLIC.PART_D_EVENTS AS
SELECT 
    *,
    METADATA$FILENAME as SOURCE_FILE,
    CURRENT_TIMESTAMP() as LOAD_TIMESTAMP
FROM MEDICARE_BRONZE_DB.PUBLIC.PART_D_EVENTS_EXTERNAL;
"""

create_quality_checks = """
-- Create a view for data quality metrics
CREATE OR REPLACE VIEW MEDICARE_BRONZE_DB.PUBLIC.DATA_QUALITY_CHECKS AS
WITH quality_metrics AS (
    -- Beneficiary quality checks
    SELECT 
        'BENEFICIARY' as TABLE_NAME,
        'Missing DOB' as CHECK_TYPE,
        COUNT(CASE WHEN $1:BENE_BIRTH_DT IS NULL THEN 1 END) as FAILED_COUNT,
        COUNT(*) as TOTAL_COUNT
    FROM MEDICARE_BRONZE_DB.PUBLIC.BENEFICIARY_EXTERNAL
    
    UNION ALL
    
    -- Claims quality checks
    SELECT 
        'CLAIMS' as TABLE_NAME,
        'Missing Dates' as CHECK_TYPE,
        COUNT(CASE WHEN $1:CLM_FROM_DT IS NULL OR $1:CLM_THRU_DT IS NULL THEN 1 END) as FAILED_COUNT,
        COUNT(*) as TOTAL_COUNT
    FROM MEDICARE_BRONZE_DB.PUBLIC.CLAIMS_EXTERNAL
    
    UNION ALL
    
    -- Part D quality checks
    SELECT 
        'PART_D_EVENTS' as TABLE_NAME,
        'Missing RX' as CHECK_TYPE,
        COUNT(CASE WHEN $1:PROD_SRVC_ID IS NULL THEN 1 END) as FAILED_COUNT,
        COUNT(*) as TOTAL_COUNT
    FROM MEDICARE_BRONZE_DB.PUBLIC.PART_D_EVENTS_EXTERNAL
)
SELECT 
    TABLE_NAME,
    CHECK_TYPE,
    FAILED_COUNT,
    TOTAL_COUNT,
    ROUND((FAILED_COUNT / NULLIF(TOTAL_COUNT, 0)) * 100, 2) as FAILURE_PERCENTAGE
FROM quality_metrics
ORDER BY 
    TABLE_NAME,
    CHECK_TYPE;
"""

validate_tables = """
-- First statement: Check row counts and distinct beneficiaries
SELECT 
    'BENEFICIARY' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT $1:DESYNPUF_ID) as distinct_beneficiaries
FROM MEDICARE_BRONZE_DB.PUBLIC.BENEFICIARY_EXTERNAL
UNION ALL
SELECT 
    'CLAIMS' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT $1:DESYNPUF_ID) as distinct_beneficiaries
FROM MEDICARE_BRONZE_DB.PUBLIC.CLAIMS_EXTERNAL
UNION ALL
SELECT 
    'PART_D_EVENTS' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT $1:DESYNPUF_ID) as distinct_beneficiaries
FROM MEDICARE_BRONZE_DB.PUBLIC.PART_D_EVENTS_EXTERNAL;

-- Second statement: Check schema information
SELECT 
    TABLE_NAME,
    TABLE_SCHEMA,
    TABLE_CATALOG,
    COMMENT,
    ROW_COUNT,
    BYTES
FROM MEDICARE_BRONZE_DB.INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME IN ('BENEFICIARY_EXTERNAL', 'CLAIMS_EXTERNAL', 'PART_D_EVENTS_EXTERNAL')
ORDER BY TABLE_NAME;
"""

with DAG(
    'medicare_bronze_load',
    default_args=default_args,
    description='Load Medicare data into bronze layer',
    schedule_interval=None,
    catchup=False,
    tags=['medicare', 'bronze', 'load']
) as dag:

    # Create file format
    create_format = SQLExecuteQueryOperator(
        task_id='create_file_format',
        conn_id='snowflake_default',
        sql=create_file_format,
        hook_params={
            'warehouse': 'MEDICARE_DEV_WH',
            'database': 'MEDICARE_BRONZE_DB',
            'schema': 'PUBLIC'
        }
    )

    # Create external stage
    create_stage = SQLExecuteQueryOperator(
        task_id='create_external_stage',
        conn_id='snowflake_default',
        sql=create_external_stage,
        hook_params={
            'warehouse': 'MEDICARE_DEV_WH',
            'database': 'MEDICARE_BRONZE_DB',
            'schema': 'PUBLIC'
        }
    )

    # Create external tables
    create_ext_tables = SQLExecuteQueryOperator(
        task_id='create_external_tables',
        conn_id='snowflake_default',
        sql=create_external_tables,
        hook_params={
            'warehouse': 'MEDICARE_DEV_WH',
            'database': 'MEDICARE_BRONZE_DB',
            'schema': 'PUBLIC'
        }
    )

    # Create materialized tables
    create_mat_tables = SQLExecuteQueryOperator(
        task_id='create_materialized_tables',
        conn_id='snowflake_default',
        sql=create_materialized_tables,
        hook_params={
            'warehouse': 'MEDICARE_DEV_WH',
            'database': 'MEDICARE_BRONZE_DB',
            'schema': 'PUBLIC'
        }
    )

    # Create quality check views
    create_dq_views = SQLExecuteQueryOperator(
        task_id='create_quality_check_views',
        conn_id='snowflake_default',
        sql=create_quality_checks,
        hook_params={
            'warehouse': 'MEDICARE_DEV_WH',
            'database': 'MEDICARE_BRONZE_DB',
            'schema': 'PUBLIC'
        }
    )

    # Validate the loaded data
    validate_data = SQLExecuteQueryOperator(
        task_id='validate_loaded_data',
        conn_id='snowflake_default',
        sql=validate_tables,
        hook_params={
            'warehouse': 'MEDICARE_DEV_WH',
            'database': 'MEDICARE_BRONZE_DB',
            'schema': 'PUBLIC'
        }
    )

    # Set up task dependencies
    chain(
        create_format,
        create_stage,
        create_ext_tables,
        create_mat_tables,
        create_dq_views,
        validate_data
    )