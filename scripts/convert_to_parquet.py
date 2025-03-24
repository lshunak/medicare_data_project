import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_path', 'target_path', 'delimiter'])

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get parameters
source_path = args['source_path']
target_path = args['target_path']
delimiter = args.get('delimiter', '|')

print(f"Starting conversion from {source_path} to {target_path}")

try:
    # Read CSV using Spark DataFrame API
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("delimiter", delimiter) \
        .option("inferSchema", "true") \
        .option("nullValue", "") \
        .load(source_path)

    # Log schema and count
    print(f"Loaded {df.count()} records")
    print("Schema:")
    df.printSchema()

    # Write as Parquet with optimization
    df.write.mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .save(target_path)

    print(f"Successfully converted data to Parquet at {target_path}")
    job.commit()

except Exception as e:
    print(f"Error during conversion: {str(e)}")
    raise