from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'lshunak',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1)
}

with DAG(
    'medicare_conversion_to_parquet',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['medicare', 'etl', 'parquet'],
    concurrency=3  
) as dag:

    # Beneficiary conversion task
    convert_beneficiary = GlueJobOperator(
        task_id='convert_beneficiary_to_parquet',
        job_name='medicare_beneficiary_to_parquet',
        script_location='s3://lshunak-cms-bucket/scripts/csv_to_parquet.py',
        script_args={
            '--source_path': 's3://lshunak-cms-bucket/raw/beneficiary/',
            '--target_path': 's3://lshunak-cms-bucket/processed/beneficiary/',
            '--delimiter': '|'
        },
        iam_role_name='AWSGlueServiceRole-MedicareCatalog',
        create_job_kwargs={
            "GlueVersion": "4.0",
            "NumberOfWorkers": 2,
            "WorkerType": "G.1X",
            "DefaultArguments": {
                "--job-language": "python",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics": "true",
                "--TempDir": "s3://lshunak-cms-bucket/temp/",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": "s3://lshunak-cms-bucket/spark-logs/"
            }
        }
    )

    # Claims conversion task
    convert_claims = GlueJobOperator(
        task_id='convert_claims_to_parquet',
        job_name='medicare_claims_to_parquet',
        script_location='s3://lshunak-cms-bucket/scripts/csv_to_parquet.py',
        script_args={
            '--source_path': 's3://lshunak-cms-bucket/raw/claims/',
            '--target_path': 's3://lshunak-cms-bucket/processed/claims/',
            '--delimiter': '|'
        },
        iam_role_name='AWSGlueServiceRole-MedicareCatalog',
        create_job_kwargs={
            "GlueVersion": "4.0",
            "NumberOfWorkers": 2,
            "WorkerType": "G.1X",
            "DefaultArguments": {
                "--job-language": "python",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics": "true",
                "--TempDir": "s3://lshunak-cms-bucket/temp/",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": "s3://lshunak-cms-bucket/spark-logs/"
            }
        }
    )

    # Part D conversion task
    convert_part_d = GlueJobOperator(
        task_id='convert_part_d_to_parquet',
        job_name='medicare_part_d_to_parquet',
        script_location='s3://lshunak-cms-bucket/scripts/csv_to_parquet.py',
        script_args={
            '--source_path': 's3://lshunak-cms-bucket/raw/part_d/',
            '--target_path': 's3://lshunak-cms-bucket/processed/part_d/',
            '--delimiter': '|'
        },
        iam_role_name='AWSGlueServiceRole-MedicareCatalog',
        create_job_kwargs={
            "GlueVersion": "4.0",
            "NumberOfWorkers": 2,
            "WorkerType": "G.1X",
            "DefaultArguments": {
                "--job-language": "python",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics": "true",
                "--TempDir": "s3://lshunak-cms-bucket/temp/",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": "s3://lshunak-cms-bucket/spark-logs/"
            }
        }
    )

   
    [convert_beneficiary, convert_claims, convert_part_d]