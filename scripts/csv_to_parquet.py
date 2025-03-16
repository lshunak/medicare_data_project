import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def process_csv_to_parquet():
    """Convert CSV files to Parquet format"""
    # Get job arguments
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 
        'source_path',
        'target_path',
        'partition_by'
    ])
    
    # Initialize Spark and Glue
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    try:
        print(f"Starting conversion from CSV to Parquet")
        print(f"Source: {args['source_path']}")
        print(f"Target: {args['target_path']}")
        
        # Read source data directly from S3
        dyf = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [args['source_path']], "recurse": True},
            format="csv",
            format_options={
                "withHeader": True,
                "separator": ","
            }
        )
        
        # Convert to DataFrame
        df = dyf.toDF()
        
        # Get row count for validation
        row_count = df.count()
        print(f"Source data row count: {row_count}")
        
        # Write as Parquet
        writer = df.write.mode("overwrite").option("compression", "snappy")
        
        # Add partitioning if specified
        if 'partition_by' in args and args['partition_by'].strip():
            partition_columns = [col.strip() for col in args['partition_by'].split(",")]
            print(f"Partitioning by columns: {partition_columns}")
            writer = writer.partitionBy(*partition_columns)
        
        # Write the data
        writer.parquet(args['target_path'])
        
        print(f"Conversion complete: CSV to Parquet")
        return 0
    except Exception as e:
        print(f"Error in CSV to Parquet conversion: {str(e)}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    process_csv_to_parquet()