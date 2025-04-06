# dags/medicare_init_setup.py
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'lshunak',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# SQL commands for different setup steps
create_warehouse = """
CREATE WAREHOUSE IF NOT EXISTS MEDICARE_DEV_WH 
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;
"""

create_databases = """
-- Create separate databases for each layer
CREATE DATABASE IF NOT EXISTS MEDICARE_BRONZE_DB;
CREATE DATABASE IF NOT EXISTS MEDICARE_SILVER_DB;
CREATE DATABASE IF NOT EXISTS MEDICARE_GOLD_DB;
"""

create_schemas = """
-- Create default schemas in each database
USE DATABASE MEDICARE_BRONZE_DB;
CREATE SCHEMA IF NOT EXISTS PUBLIC;

USE DATABASE MEDICARE_SILVER_DB;
CREATE SCHEMA IF NOT EXISTS PUBLIC;

USE DATABASE MEDICARE_GOLD_DB;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
"""

grant_permissions = """
-- Warehouse permissions
GRANT USAGE ON WAREHOUSE MEDICARE_DEV_WH TO ROLE ACCOUNTADMIN;

-- Database permissions
GRANT USAGE ON DATABASE MEDICARE_BRONZE_DB TO ROLE ACCOUNTADMIN;
GRANT USAGE ON DATABASE MEDICARE_SILVER_DB TO ROLE ACCOUNTADMIN;
GRANT USAGE ON DATABASE MEDICARE_GOLD_DB TO ROLE ACCOUNTADMIN;

-- Schema permissions for each database
GRANT USAGE ON SCHEMA MEDICARE_BRONZE_DB.PUBLIC TO ROLE ACCOUNTADMIN;
GRANT CREATE TABLE ON SCHEMA MEDICARE_BRONZE_DB.PUBLIC TO ROLE ACCOUNTADMIN;
GRANT CREATE VIEW ON SCHEMA MEDICARE_BRONZE_DB.PUBLIC TO ROLE ACCOUNTADMIN;

GRANT USAGE ON SCHEMA MEDICARE_SILVER_DB.PUBLIC TO ROLE ACCOUNTADMIN;
GRANT CREATE TABLE ON SCHEMA MEDICARE_SILVER_DB.PUBLIC TO ROLE ACCOUNTADMIN;
GRANT CREATE VIEW ON SCHEMA MEDICARE_SILVER_DB.PUBLIC TO ROLE ACCOUNTADMIN;

GRANT USAGE ON SCHEMA MEDICARE_GOLD_DB.PUBLIC TO ROLE ACCOUNTADMIN;
GRANT CREATE TABLE ON SCHEMA MEDICARE_GOLD_DB.PUBLIC TO ROLE ACCOUNTADMIN;
GRANT CREATE VIEW ON SCHEMA MEDICARE_GOLD_DB.PUBLIC TO ROLE ACCOUNTADMIN;
"""

with DAG(
    'medicare_init_setup',
    default_args=default_args,
    description='Initialize Medicare Data Project Snowflake Environment',
    schedule_interval=None,  # Manual trigger only
    catchup=False
) as dag:

    # Task 1: Create Warehouse
    create_wh = SQLExecuteQueryOperator(
        task_id='create_warehouse',
        conn_id='snowflake_default',
        sql=create_warehouse,
        hook_params={
            'warehouse': 'MEDICARE_DEV_WH',
            'role': 'ACCOUNTADMIN'
        }
    )

    # Task 2: Create Databases
    create_dbs = SQLExecuteQueryOperator(
        task_id='create_databases',
        conn_id='snowflake_default',
        sql=create_databases,
        hook_params={
            'warehouse': 'MEDICARE_DEV_WH',
            'role': 'ACCOUNTADMIN'
        }
    )

    # Task 3: Create Schemas
    create_schm = SQLExecuteQueryOperator(
        task_id='create_schemas',
        conn_id='snowflake_default',
        sql=create_schemas,
        hook_params={
            'warehouse': 'MEDICARE_DEV_WH',
            'role': 'ACCOUNTADMIN'
        }
    )

    # Task 4: Grant Permissions
    grant_perms = SQLExecuteQueryOperator(
        task_id='grant_permissions',
        conn_id='snowflake_default',
        sql=grant_permissions,
        hook_params={
            'warehouse': 'MEDICARE_DEV_WH',
            'role': 'ACCOUNTADMIN'
        }
    )

    # Set up task dependencies
    create_wh >> create_dbs >> create_schm >> grant_perms