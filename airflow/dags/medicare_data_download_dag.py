"""
Medicare Data Download, Extract, and S3 Upload DAG
-------------------------------------------------
Downloads Medicare data from CMS.gov, extracts files locally, uploads to S3,
converts to Parquet, and catalogs for data lake usage.

Triggering: Manual only (no schedule)
Author: lshunak
Date: 2025-03-20
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
import os
import requests
import logging
import zipfile
import shutil
from datetime import datetime, timedelta
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

# Configuration - Direct from your provided URLs
DATA_SOURCES = {
    'beneficiary': {
        'url': "https://data.cms.gov/sites/default/files/2023-04/250e6ca0-3515-4767-957c-5528bfcee75c/All%20Beneficiary%20Years.zip",
        'is_zip': True,
        'local_path': "beneficiary",
        's3_prefix': 'raw/beneficiary/'
    },
    'claims': {
        'url': "https://data.cms.gov/sites/default/files/2023-04/f67c2406-4413-4ace-817d-b170cbdd0a3e/All%20FFS%20Claims.zip",
        'is_zip': True,
        'local_path': "claims",
        's3_prefix': 'raw/claims/'
    },
    'part_d': {
        'url': "https://data.cms.gov/sites/default/files/2023-04/a3969dcf-0799-49fe-8380-9eef788d5ac4/pde.csv",
        'is_zip': False,
        'local_path': "part_d",
        's3_prefix': 'raw/part_d/'
    }
}

BASE_DIR = os.path.expanduser('~/Documents/medicare_data_project/data/raw')
S3_BUCKET = 'lshunak-cms-bucket'  

# task functions
def download_file(url, output_path, **kwargs):
    """
    Download a file from a URL and save to the specified path
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        logger.info(f"Downloading from {url} to {output_path}")
        
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(output_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        file_size = os.path.getsize(output_path)
        logger.info(f"Successfully downloaded file, size: {file_size} bytes")
        
        return output_path
        
    except Exception as e:
        logger.error(f"Error downloading file: {str(e)}")
        raise

def extract_zip(zip_path, extract_dir, **kwargs):
    """
    Extract a ZIP file to the specified directory
    """
    try:
        # Create extract directory if it doesn't exist
        os.makedirs(extract_dir, exist_ok=True)
        
        logger.info(f"Extracting {zip_path} to {extract_dir}")
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        # List the extracted files
        extracted_files = []
        for root, _, files in os.walk(extract_dir):
            for file in files:
                file_path = os.path.join(root, file)
                # Skip the original zip file in the list of extracted files
                if file_path != zip_path:
                    extracted_files.append(file_path)
        
        logger.info(f"Successfully extracted {len(extracted_files)} files")
        
        return extracted_files
        
    except Exception as e:
        logger.error(f"Error extracting ZIP file: {str(e)}")
        raise

def upload_files_to_s3(files, s3_prefix, **kwargs):
    """
    Upload a list of files to S3
    """
    try:
        s3_hook = S3Hook()
        uploaded_count = 0
        
        for file_path in files:
            # Get the base filename to use in S3
            base_name = os.path.basename(file_path)
            s3_key = f"{s3_prefix}{base_name}"
            
            # Skip uploading zip files
            if base_name.endswith('.zip'):
                logger.info(f"Skipping upload of ZIP file: {file_path}")
                continue
                
            logger.info(f"Uploading {file_path} to s3://{S3_BUCKET}/{s3_key}")
            
            # Upload file with metadata
            s3_hook.load_file(
                filename=file_path,
                key=s3_key,
                bucket_name=S3_BUCKET,
                replace=True,
                encrypt=True
            )
            
            uploaded_count += 1
        
        logger.info(f"Successfully uploaded {uploaded_count} files to S3")
        return uploaded_count
        
    except Exception as e:
        logger.error(f"Error uploading files to S3: {str(e)}")
        raise

def download_beneficiary_data(**kwargs):
    """Download Beneficiary data zip file"""
    source = DATA_SOURCES['beneficiary']
    output_dir = os.path.join(BASE_DIR, source['local_path'])
    zip_path = os.path.join(output_dir, "beneficiary.zip")
    
    # Download the file
    download_file(source['url'], zip_path)
    
    # Save the path for the extraction task
    kwargs['ti'].xcom_push(key='beneficiary_zip_path', value=zip_path)
    kwargs['ti'].xcom_push(key='beneficiary_extract_dir', value=output_dir)
    
    return zip_path

def extract_beneficiary_data(**kwargs):
    """Extract Beneficiary data from zip file"""
    ti = kwargs['ti']
    zip_path = ti.xcom_pull(key='beneficiary_zip_path', task_ids='download_beneficiary_data')
    extract_dir = ti.xcom_pull(key='beneficiary_extract_dir', task_ids='download_beneficiary_data')
    
    # Extract the zip file
    extracted_files = extract_zip(zip_path, extract_dir)
    
    # Save the list of extracted files for the upload task
    kwargs['ti'].xcom_push(key='beneficiary_files', value=extracted_files)
    
    return len(extracted_files)

def upload_beneficiary_to_s3(**kwargs):
    """Upload extracted Beneficiary files to S3"""
    ti = kwargs['ti']
    files = ti.xcom_pull(key='beneficiary_files', task_ids='extract_beneficiary_data')
    s3_prefix = DATA_SOURCES['beneficiary']['s3_prefix']
    
    # Upload files to S3
    return upload_files_to_s3(files, s3_prefix)

def cleanup_beneficiary_data(**kwargs):
    """Clean up beneficiary data after successful S3 upload"""
    source = DATA_SOURCES['beneficiary']
    output_dir = os.path.join(BASE_DIR, source['local_path'])
    
    logger.info(f"Cleaning up local data in {output_dir}")
    
    # Count files before deletion
    file_count = sum(len(files) for _, _, files in os.walk(output_dir))
    
    try:
        # Remove all files in the directory but keep the directory itself
        for root, dirs, files in os.walk(output_dir):
            for file in files:
                file_path = os.path.join(root, file)
                os.remove(file_path)
                logger.info(f"Removed file: {file_path}")
        
        logger.info(f"Successfully cleaned up {file_count} files in {output_dir}")
        return file_count
    except Exception as e:
        logger.error(f"Error cleaning up {output_dir}: {str(e)}")
        raise

def download_claims_data(**kwargs):
    """Download Claims data zip file"""
    source = DATA_SOURCES['claims']
    output_dir = os.path.join(BASE_DIR, source['local_path'])
    zip_path = os.path.join(output_dir, "claims.zip")
    
    # Download the file
    download_file(source['url'], zip_path)
    
    # Save the path for the extraction task
    kwargs['ti'].xcom_push(key='claims_zip_path', value=zip_path)
    kwargs['ti'].xcom_push(key='claims_extract_dir', value=output_dir)
    
    return zip_path

def extract_claims_data(**kwargs):
    """Extract Claims data from zip file"""
    ti = kwargs['ti']
    zip_path = ti.xcom_pull(key='claims_zip_path', task_ids='download_claims_data')
    extract_dir = ti.xcom_pull(key='claims_extract_dir', task_ids='download_claims_data')
    
    # Extract the zip file
    extracted_files = extract_zip(zip_path, extract_dir)
    
    # Save the list of extracted files for the upload task
    kwargs['ti'].xcom_push(key='claims_files', value=extracted_files)
    
    return len(extracted_files)

def upload_claims_to_s3(**kwargs):
    """Upload extracted Claims files to S3"""
    ti = kwargs['ti']
    files = ti.xcom_pull(key='claims_files', task_ids='extract_claims_data')
    s3_prefix = DATA_SOURCES['claims']['s3_prefix']
    
    # Upload files to S3
    return upload_files_to_s3(files, s3_prefix)

def cleanup_claims_data(**kwargs):
    """Clean up claims data after successful S3 upload"""
    source = DATA_SOURCES['claims']
    output_dir = os.path.join(BASE_DIR, source['local_path'])
    
    logger.info(f"Cleaning up local data in {output_dir}")
    
    file_count = sum(len(files) for _, _, files in os.walk(output_dir))
    
    try:
        for root, dirs, files in os.walk(output_dir):
            for file in files:
                file_path = os.path.join(root, file)
                os.remove(file_path)
                logger.info(f"Removed file: {file_path}")
        
        logger.info(f"Successfully cleaned up {file_count} files in {output_dir}")
        return file_count
    except Exception as e:
        logger.error(f"Error cleaning up {output_dir}: {str(e)}")
        raise

def download_part_d_data(**kwargs):
    """Download Part D data CSV file"""
    source = DATA_SOURCES['part_d']
    output_dir = os.path.join(BASE_DIR, source['local_path'])
    file_path = os.path.join(output_dir, "pde.csv")
    
    # Download the file
    download_file(source['url'], file_path)
    
    # Save the path for the upload task
    kwargs['ti'].xcom_push(key='part_d_file', value=file_path)
    
    return file_path

def upload_part_d_to_s3(**kwargs):
    """Upload Part D CSV file to S3"""
    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='part_d_file', task_ids='download_part_d_data')
    s3_prefix = DATA_SOURCES['part_d']['s3_prefix']
    
    # Upload file to S3
    return upload_files_to_s3([file_path], s3_prefix)

def cleanup_part_d_data(**kwargs):
    """Clean up Part D data after successful S3 upload"""
    source = DATA_SOURCES['part_d']
    output_dir = os.path.join(BASE_DIR, source['local_path'])
    
    logger.info(f"Cleaning up local data in {output_dir}")
    
    # Count files before deletion
    file_count = sum(len(files) for _, _, files in os.walk(output_dir))
    
    try:
        # Remove all files in the directory but keep the directory itself
        for root, dirs, files in os.walk(output_dir):
            for file in files:
                file_path = os.path.join(root, file)
                os.remove(file_path)
                logger.info(f"Removed file: {file_path}")
        
        logger.info(f"Successfully cleaned up {file_count} files in {output_dir}")
        return file_count
    except Exception as e:
        logger.error(f"Error cleaning up {output_dir}: {str(e)}")
        raise

# Define default arguments
default_args = {
    'owner': 'lshunak',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
with DAG(
    'medicare_data_extract_s3',
    default_args=default_args,
    description='Download, extract Medicare data and upload to S3',
    schedule_interval=None,  
    start_date=days_ago(1),
    catchup=False,
    tags=['medicare', 'download', 'cms', 's3'],
) as dag:

    # Task 0: Create base directory
    create_base_dir = BashOperator(
        task_id='create_base_directory',
        bash_command=f'mkdir -p {BASE_DIR}',
    )
    
    # Create data subdirectories
    create_subdirs = BashOperator(
        task_id='create_data_subdirectories',
        bash_command=f'mkdir -p {BASE_DIR}/beneficiary {BASE_DIR}/claims {BASE_DIR}/part_d',
    )

    # Beneficiary data tasks
    download_beneficiary = PythonOperator(
        task_id='download_beneficiary_data',
        python_callable=download_beneficiary_data,
    )

    extract_beneficiary = PythonOperator(
        task_id='extract_beneficiary_data',
        python_callable=extract_beneficiary_data,
    )
    
    upload_beneficiary = PythonOperator(
        task_id='upload_beneficiary_to_s3',
        python_callable=upload_beneficiary_to_s3,
    )
    
    cleanup_beneficiary = PythonOperator(
        task_id='cleanup_beneficiary_data',
        python_callable=cleanup_beneficiary_data,
        trigger_rule='all_success',  # Only run if upload succeeds
    )

    # Claims data tasks
    download_claims = PythonOperator(
        task_id='download_claims_data',
        python_callable=download_claims_data,
    )

    extract_claims = PythonOperator(
        task_id='extract_claims_data',
        python_callable=extract_claims_data,
    )
    
    upload_claims = PythonOperator(
        task_id='upload_claims_to_s3',
        python_callable=upload_claims_to_s3,
    )
    
    cleanup_claims = PythonOperator(
        task_id='cleanup_claims_data',
        python_callable=cleanup_claims_data,
        trigger_rule='all_success',  # Only run if upload succeeds
    )

    # Part D data tasks
    download_part_d = PythonOperator(
        task_id='download_part_d_data',
        python_callable=download_part_d_data,
    )
    
    upload_part_d = PythonOperator(
        task_id='upload_part_d_to_s3',
        python_callable=upload_part_d_to_s3,
    )
    
    # Add cleanup task
    cleanup_part_d = PythonOperator(
        task_id='cleanup_part_d_data',
        python_callable=cleanup_part_d_data,
        trigger_rule='all_success',  # Only run if upload succeeds
    )

    catalog_all_data = GlueCrawlerOperator(
        task_id='catalog_medicare_data',
        aws_conn_id='aws_default',
        config={
            'Name': 'medicare_data_crawler',
            'Role': 'AWSGlueServiceRole-MedicareCatalog',
            'DatabaseName': 'medicare_catalog',
            'Targets': {
                'S3Targets': [
                    {'Path': f's3://{S3_BUCKET}/raw/'},  # This will crawl all subdirectories
                ]
            }
        }
    )

    # Define task dependencies
    create_base_dir >> create_subdirs
    
    # Beneficiary workflow
    create_subdirs >> download_beneficiary >> extract_beneficiary >> upload_beneficiary >> [cleanup_beneficiary, catalog_all_data]
    
    # Claims workflow
    create_subdirs >> download_claims >> extract_claims >> upload_claims >> [cleanup_claims, catalog_all_data]
    
    # Part D workflow (direct CSV, no extraction needed)
    create_subdirs >> download_part_d >> upload_part_d >> [cleanup_part_d, catalog_all_data]
    
