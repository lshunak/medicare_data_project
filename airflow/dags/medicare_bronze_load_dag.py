from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.baseoperator import chain
from datetime import datetime, timedelta

# Default arguments configuration
default_args = {
    'owner': 'lshunak',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 14, 9, 2, 46),  # Current timestamp
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Just ensure the database exists
create_database = """
CREATE DATABASE IF NOT EXISTS MEDICARE_RAW_DB;
"""

create_file_format = """
CREATE OR REPLACE FILE FORMAT MEDICARE_RAW_DB.PUBLIC.PARQUET_FORMAT
    TYPE = 'PARQUET'
    BINARY_AS_TEXT = FALSE
    TRIM_SPACE = TRUE;
"""

create_external_stage = """
CREATE OR REPLACE STAGE MEDICARE_RAW_DB.PUBLIC.MEDICARE_STAGE
    URL = 's3://lshunak-cms-bucket/processed/'
    CREDENTIALS = (AWS_KEY_ID = '{{ conn.aws_default.login }}'
                  AWS_SECRET_KEY = '{{ conn.aws_default.password }}')
    FILE_FORMAT = MEDICARE_RAW_DB.PUBLIC.PARQUET_FORMAT;
"""

create_external_tables = """
-- Beneficiary external table
CREATE OR REPLACE EXTERNAL TABLE MEDICARE_RAW_DB.PUBLIC.BENEFICIARY_EXTERNAL
    WITH LOCATION = @MEDICARE_STAGE/beneficiary/
    FILE_FORMAT = (TYPE = PARQUET)
    AUTO_REFRESH = TRUE
    PATTERN = '.*[.]parquet'
    ENABLE_SCHEMA_EVOLUTION = TRUE;

-- Claims external table
CREATE OR REPLACE EXTERNAL TABLE MEDICARE_RAW_DB.PUBLIC.CLAIMS_EXTERNAL
    WITH LOCATION = @MEDICARE_STAGE/claims/
    FILE_FORMAT = (TYPE = PARQUET)
    AUTO_REFRESH = TRUE
    PATTERN = '.*/.*[.]parquet'
    ENABLE_SCHEMA_EVOLUTION = TRUE;

-- Part D Events external table
CREATE OR REPLACE EXTERNAL TABLE MEDICARE_RAW_DB.PUBLIC.PART_D_EVENTS_EXTERNAL
    WITH LOCATION = @MEDICARE_STAGE/part_d/
    FILE_FORMAT = (TYPE = PARQUET)
    AUTO_REFRESH = TRUE
    PATTERN = '.*[.]parquet'
    ENABLE_SCHEMA_EVOLUTION = TRUE;
"""

create_quality_checks = """
CREATE OR REPLACE VIEW MEDICARE_RAW_DB.PUBLIC.DATA_QUALITY_CHECKS AS
WITH quality_metrics AS (
    -- Beneficiary quality checks
    SELECT 
        'BENEFICIARY' as TABLE_NAME,
        'Missing DOB' as CHECK_TYPE,
        COUNT(CASE WHEN $1:BENE_BIRTH_DT IS NULL THEN 1 END) as FAILED_COUNT,
        COUNT(*) as TOTAL_COUNT
    FROM MEDICARE_RAW_DB.PUBLIC.BENEFICIARY_EXTERNAL
    
    UNION ALL
    
    -- Claims quality checks
    SELECT 
        'CLAIMS' as TABLE_NAME,
        'Missing Dates' as CHECK_TYPE,
        COUNT(CASE WHEN $1:CLM_FROM_DT IS NULL OR $1:CLM_THRU_DT IS NULL THEN 1 END) as FAILED_COUNT,
        COUNT(*) as TOTAL_COUNT
    FROM MEDICARE_RAW_DB.PUBLIC.CLAIMS_EXTERNAL
    
    UNION ALL
    
    -- Part D quality checks
    SELECT 
        'PART_D_EVENTS' as TABLE_NAME,
        'Missing RX' as CHECK_TYPE,
        COUNT(CASE WHEN $1:PROD_SRVC_ID IS NULL THEN 1 END) as FAILED_COUNT,
        COUNT(*) as TOTAL_COUNT
    FROM MEDICARE_RAW_DB.PUBLIC.PART_D_EVENTS_EXTERNAL
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
SELECT 
    'BENEFICIARY' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT $1:BENE_ID) as distinct_beneficiaries
FROM MEDICARE_RAW_DB.PUBLIC.BENEFICIARY_EXTERNAL
UNION ALL
SELECT 
    'CLAIMS' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT $1:BENE_ID) as distinct_beneficiaries
FROM MEDICARE_RAW_DB.PUBLIC.CLAIMS_EXTERNAL
UNION ALL
SELECT 
    'PART_D_EVENTS' as table_name,
    COUNT(*) as row_count,
    COUNT(DISTINCT $1:BENE_ID) as distinct_beneficiaries
FROM MEDICARE_RAW_DB.PUBLIC.PART_D_EVENTS_EXTERNAL;
"""

# DAG definition
with DAG(
    'medicare_setup_infrastructure',
    default_args=default_args,
    description='Set up Medicare infrastructure in Snowflake',
    schedule_interval=None,
    catchup=False,
    tags=['medicare', 'infrastructure', 'setup']
) as dag:

    # Task definitions
    create_db = SQLExecuteQueryOperator(
        task_id='create_database',
        conn_id='snowflake_default',
        sql=create_database,
        hook_params={
            'warehouse': 'MEDICARE_DEV_WH',
            'schema': 'PUBLIC'
        }
    )

    create_format = SQLExecuteQueryOperator(
        task_id='create_file_format',
        conn_id='snowflake_default',
        sql=create_file_format,
        hook_params={
            'warehouse': 'MEDICARE_DEV_WH',
            'database': 'MEDICARE_RAW_DB',
            'schema': 'PUBLIC'
        }
    )

    create_stage = SQLExecuteQueryOperator(
        task_id='create_external_stage',
        conn_id='snowflake_default',
        sql=create_external_stage,
        hook_params={
            'warehouse': 'MEDICARE_DEV_WH',
            'database': 'MEDICARE_RAW_DB',
            'schema': 'PUBLIC'
        }
    )

    create_ext_tables = SQLExecuteQueryOperator(
        task_id='create_external_tables',
        conn_id='snowflake_default',
        sql=create_external_tables,
        hook_params={
            'warehouse': 'MEDICARE_DEV_WH',
            'database': 'MEDICARE_RAW_DB',
            'schema': 'PUBLIC'
        }
    )

    create_dq_views = SQLExecuteQueryOperator(
        task_id='create_quality_check_views',
        conn_id='snowflake_default',
        sql=create_quality_checks,
        hook_params={
            'warehouse': 'MEDICARE_DEV_WH',
            'database': 'MEDICARE_RAW_DB',
            'schema': 'PUBLIC'
        }
    )

    validate_data = SQLExecuteQueryOperator(
        task_id='validate_loaded_data',
        conn_id='snowflake_default',
        sql=validate_tables,
        hook_params={
            'warehouse': 'MEDICARE_DEV_WH',
            'database': 'MEDICARE_RAW_DB',
            'schema': 'PUBLIC'
        }
    )

    # Task dependencies
    chain(
        create_db,
        create_format,
        create_stage,
        create_ext_tables,
        create_dq_views,
        validate_data
    )