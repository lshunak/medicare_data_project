"""
AWS Glue Job Script for Converting Medicare CSV Data to Parquet
--------------------------------------------------------------
This script converts Medicare CSV data stored in S3 to Parquet format,
organizing by data type for efficient querying.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import current_timestamp, lit, when, col
import boto3

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, 
                         ['JOB_NAME', 
                          'src_bucket', 
                          'dst_bucket', 
                          'data_type'])

# Define parameters
src_bucket = args['src_bucket']
dst_bucket = args['dst_bucket']
data_type = args['data_type']  # beneficiary, claims, or part_d
process_all_claim_types = args.get('process_all_claim_types', 'false').lower() == 'true'

# S3 client for listing objects
s3 = boto3.client('s3')

# Log job parameters
print(f"Job Parameters:")
print(f"  Source Bucket: {src_bucket}")
print(f"  Destination Bucket: {dst_bucket}")
print(f"  Data Type: {data_type}")
print(f"  Process All Claim Types: {process_all_claim_types}")

# Process function with better handling for different data types
def process_data(source_path, destination_path, claim_type=None):
    print(f"Processing: {source_path} -> {destination_path}")
    
    # Set appropriate CSV options based on data type
    csv_options = {
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": True
    }
    
    if data_type == 'part_d':
        # Part D data might need special handling
        csv_options["inferSchema"] = True
    
    # Read source data
    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [source_path]},
        format="csv",
        format_options=csv_options
    )
    
    # Check if data was loaded
    record_count = datasource.count()
    print(f"Loaded {record_count} records from {source_path}")
    
    if record_count > 0:
        # Convert to DataFrame for transformations
        df = datasource.toDF()
        
        # Add metadata columns
        df = df.withColumn("processed_at", current_timestamp())
        
        if claim_type:
            df = df.withColumn("claim_type", lit(claim_type))
        
        # Convert back to DynamicFrame and write to S3 as Parquet
        output_frame = DynamicFrame.fromDF(df, glueContext, "output_frame")
        
        # Write the data in Parquet format
        sink = glueContext.write_dynamic_frame.from_options(
            frame=output_frame