import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'source_path', 
    'target_path',
    'file_format',
    'delimiter'
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get parameters
source_path = args['source_path']
target_path = args['target_path']
file_format = args.get('file_format', 'csv')
delimiter = args.get('delimiter', '|')  # Default to pipe delimiter for Medicare data

print(f"Converting {file_format} files from {source_path} to Parquet at {target_path}")
print(f"Using delimiter: '{delimiter}'")

# Configure read options based on file format
read_options = {}
if file_format.lower() == 'csv':
    read_options = {
        "header": "true",
        "delimiter": delimiter,
        "inferSchema": "true",
        "nullValue": ""
    }

# Read input data
input_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [source_path],
        "recurse": True
    },
    format=file_format,
    format_options=read_options
)

# Count records
record_count = input_dyf.count()
print(f"Read {record_count} records from source")

# Print schema for logging
print("Source schema:")
input_dyf.printSchema()

# Apply any transformations here if needed
# For Medicare data, we might want to handle specific data type conversions
# or normalization logic in the future

# Convert to Parquet with optimizations
print("Converting to Parquet format...")
glueContext.write_dynamic_frame.from_options(
    frame=input_dyf,
    connection_type="s3",
    connection_options={
        "path": target_path,
        "partitionKeys": []  # No partitioning for now
    },
    format="parquet",
    format_options={
        "compression": "snappy"  # Use Snappy compression for good balance of size and speed
    },
    transformation_ctx="write_parquet"
)

print(f"Successfully converted {record_count} records to Parquet at {target_path}")
job.commit()