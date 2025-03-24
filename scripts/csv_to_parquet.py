import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

print("Starting Glue Job...")

try:
    # Get job parameters
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'source_path',
        'target_path'
    ])
    
    print(f"Arguments received: {args}")
    
    # Initialize job
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    print(f"Reading CSV from: {args['source_path']}")
    
    # Read CSV using spark.read first
    input_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(args['source_path'])
    
    print(f"DataFrame schema: {input_df.schema}")
    print(f"Number of records: {input_df.count()}")
    
    # Convert to DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(input_df, glueContext, "nested")
    
    print("Writing to Parquet format...")
    
    # Write as Parquet
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": args['target_path']
        },
        format="parquet"
    )
    
    print("Write operation completed")
    job.commit()
    print("Job committed successfully")
    
except Exception as e:
    print(f"Error occurred: {str(e)}")
    import traceback
    print(f"Traceback: {traceback.format_exc()}")
    raise