from airflow import DAG
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import json

default_args = {
    'owner': 'lshunak',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1)
}

# Common crawler configuration
crawler_config = json.dumps({
    "Version": 1.0,
    "Grouping": {
        "TableGroupingPolicy": "CombineCompatibleSchemas"
    },
    "CrawlerOutput": {
        "Tables": {
            "AddOrUpdateBehavior": "MergeNewColumns"
        }
    }
})

# Dataset configurations
DATASETS = {
    'beneficiary': {
        'path': 's3://lshunak-cms-bucket/processed/beneficiary/',
        'crawler_name': 'medicare_beneficiary_crawler',
        'table_prefix': ''
    },
    'claims': {
        'path': 's3://lshunak-cms-bucket/processed/claims/',
        'crawler_name': 'medicare_claims_crawler',
        'table_prefix': ''
    },
    'part_d': {
        'path': 's3://lshunak-cms-bucket/processed/part_d/',
        'crawler_name': 'medicare_part_d_crawler',
        'table_prefix': ''
    }
}

with DAG(
    'medicare_catalog_parquet',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['medicare', 'catalog']
) as dag:

    crawlers = []
    
    # Create crawlers for each dataset
    for dataset_name, config in DATASETS.items():
        crawler = GlueCrawlerOperator(
            task_id=f'crawl_{dataset_name}_data',
            config={
                'Name': config['crawler_name'],
                'Role': 'AWSGlueServiceRole-MedicareCatalog',
                'DatabaseName': 'medicare_catalog',
                'Description': f'Crawler for Medicare {dataset_name} parquet data',
                'Targets': {
                    'S3Targets': [
                        {
                            'Path': config['path'],
                            'Exclusions': ['*.csv', '*.txt', '_temporary/*', '_committed_*']
                        }
                    ]
                },
                'TablePrefix': config['table_prefix'],
                'SchemaChangePolicy': {
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'LOG'
                },
                'Configuration': crawler_config
            }
        )
        crawlers.append(crawler)

    # Run crawlers in parallel
    crawlers