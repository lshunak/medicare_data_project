from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'lshunak',
    'depends_on_past': False,
    'retries': 0
}

with DAG(
    'medicare_conversion_to_parquet',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    convert_to_parquet = GlueJobOperator(
        task_id='convert_to_parquet',
        job_name='medicare_csv_to_parquet_v6',  # New version
        script_location='s3://lshunak-cms-bucket/scripts/csv_to_parquet.py',
        script_args={
            '--source_path': 's3://lshunak-cms-bucket/raw/beneficiary/',
            '--target_path': 's3://lshunak-cms-bucket/processed/beneficiary/'
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