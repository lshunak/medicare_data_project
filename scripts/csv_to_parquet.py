import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
from datetime import datetime

def get_s3_files(s3_path):
    """Get list of CSV files in S3 path"""
    print(f"Getting files from: {s3_path}")
    
    # Parse S3 URL
    s3_path = s3_path.rstrip('/')
    bucket = s3_path.split('//')[1].split('/')[0]
    prefix = '/'.join(s3_path.split('//')[1].split('/')[1:]) + '/'
    
    print(f"Bucket: {bucket}")
    print(f"Prefix: {prefix}")
    
    s3_client = boto3.client('s3')
    files = []
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].lower().endswith('.csv'):
                        files.append(f"s3://{bucket}/{obj['Key']}")
                        print(f"Found CSV file: s3://{bucket}/{obj['Key']}")
    except Exception as e:
        print(f"Error listing S3 files: {str(e)}")
        raise
    
    return files

def process_single_file(glueContext, source_path, target_path):
    """Process a single CSV file"""
    spark = glueContext.spark_session
    
    # Extract filename for the target path
    filename = source_path.split('/')[-1].replace('.csv', '')
    file_target_path = f"{target_path.rstrip('/')}/{filename}"
    
    print(f"\nProcessing file: {source_path}")
    print(f"Target path: {file_target_path}")
    
    try:
        # Read single CSV file with pipe delimiter
        print(f"Reading CSV file with pipe delimiter...")
        input_df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("delimiter", "|") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .load(source_path)
        
        # Print schema and count
        print(f"Schema for {filename}:")
        input_df.printSchema()
        record_count = input_df.count()
        print(f"Number of records: {record_count}")
        
        if record_count == 0:
            print(f"Warning: {filename} is empty")
            return False
        
        # Check for corrupt records
        if "_corrupt_record" in input_df.columns:
            corrupt_count = input_df.filter(input_df["_corrupt_record"].isNotNull()).count()
            if corrupt_count > 0:
                print(f"Warning: Found {corrupt_count} corrupt records in {filename}")
        
        # Convert to DynamicFrame
        print("Converting to DynamicFrame...")
        dynamic_frame = DynamicFrame.fromDF(input_df, glueContext, "nested")
        
        # Write as Parquet
        print(f"Writing to: {file_target_path}")
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options={
                "path": file_target_path
            },
            format="parquet",
            transformation_ctx=f"write_{filename}"
        )
        
        print(f"Successfully processed {filename}")
        return True
        
    except Exception as e:
        print(f"Error processing {filename}: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return False

def main():
    job_start_time = datetime.utcnow()
    print(f"Job started at {job_start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    try:
        # Get job parameters
        args = getResolvedOptions(sys.argv, [
            'JOB_NAME',
            'source_path',
            'target_path',
            'delimiter'  # Add delimiter parameter
        ])
        
        print(f"Job parameters: {args}")
        
        # Initialize Spark
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args['JOB_NAME'], args)
        
        # Configure Spark
        spark.conf.set("spark.sql.files.maxPartitionBytes", 134217728)  # 128 MB
        spark.conf.set("spark.sql.files.maxRecordsPerFile", 1000000)
        
        # Get list of files
        csv_files = get_s3_files(args['source_path'])
        print(f"\nFound {len(csv_files)} CSV files to process")
        
        if not csv_files:
            raise Exception(f"No CSV files found in {args['source_path']}")
        
        # Process files and track success/failure
        successful_files = 0
        failed_files = 0
        
        for file_path in csv_files:
            try:
                success = process_single_file(
                    glueContext=glueContext,
                    source_path=file_path,
                    target_path=args['target_path']
                )
                if success:
                    successful_files += 1
                else:
                    failed_files += 1
            except Exception as e:
                print(f"Failed to process {file_path}: {str(e)}")
                failed_files += 1
                continue
        
        # Print summary
        job_end_time = datetime.utcnow()
        duration = (job_end_time - job_start_time).total_seconds()
        print("\nJob Summary:")
        print(f"Total files processed: {len(csv_files)}")
        print(f"Successful conversions: {successful_files}")
        print(f"Failed conversions: {failed_files}")
        print(f"Total duration: {duration:.2f} seconds")
        
        if failed_files > 0:
            raise Exception(f"Job completed with {failed_files} failed files")
        
        job.commit()
        print(f"Job completed successfully at {job_end_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        
    except Exception as e:
        print(f"Job failed: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        raise

if __name__ == "__main__":
    main()