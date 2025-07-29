import sys
import re
import json
import boto3
from datetime import datetime
from dateutil import parser
 
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import *
 
# ========================
# Initialize Glue Context
# ========================
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
 
bucket_name = "my-shopify-data-12345"
failed_path = f"s3://{bucket_name}/shopify/failed/"
processed_path = f"s3://{bucket_name}/shopify/processed/"
invalid_path = f"s3://{bucket_name}/shopify/invalid/"
log_path = f"s3://{bucket_name}/shopify/logs/"
 
# ========================
# Validation Functions
# ========================
def validate_datetime(input_str):
    try:
        if input_str is None or not isinstance(input_str, str):
            return None
        cleaned_str = re.sub(r'[\-,]', ' ', input_str.strip())
        dt = parser.parse(cleaned_str, dayfirst=True, fuzzy=True)
        return dt.strftime('%d/%m/%Y %H:%M')
    except:
        return None
 
def normalize_price_to_inr(price_input, usd_to_inr_rate=82.5):
    try:
        price_str = str(price_input).strip().lower()
        is_usd = price_str.startswith('$') or 'usd' in price_str
        for symbol in ['₹', 'rs.', 'rs', '$', 'usd', ',', ' ']:
            price_str = price_str.replace(symbol, '')
        if price_str == '':
            return None
        price_float = float(price_str)
        if price_float < 0:
            return None
        if is_usd:
            price_float *= usd_to_inr_rate
        normalized = ('%.10g' % price_float)
        if 'e' in normalized or 'E' in normalized:
            normalized = format(price_float, 'f').rstrip('0').rstrip('.')
        return normalized
    except:
        return None
 
def validate_email(email_str):
    try:
        if not isinstance(email_str, str):
            return None
        email_clean = email_str.strip().replace(" ", "").lower()
        regex = r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'
        if re.match(regex, email_clean):
            return email_clean
    except:
        pass
    return None
 
def normalize_boolean(value):
    try:
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            return value == 1
        if isinstance(value, str):
            val = value.strip().lower()
            if val in {'1', 'true', 'yes', 'y', 't'}:
                return True
            elif val in {'0', 'false', 'no', 'n', 'f', ''}:
                return False
    except:
        pass
    return None
 
def validate_null(value):
    return str(value).strip().lower() not in {"", "none", "null", "nan"}
 
# ========================
# Load JSON from Failed Folder
# ========================
df = spark.read.json(failed_path)
 
# ========================
# Transform and Flag Rows
# ========================
from pyspark.sql.functions import when
 
df = df.withColumn("price_clean", udf(normalize_price_to_inr, StringType())(col("price"))) \
       .withColumn("created_at_clean", udf(validate_datetime, StringType())(col("created_at"))) \
       .withColumn("email_clean", udf(validate_email, StringType())(col("email"))) \
       .withColumn("is_active_clean", udf(normalize_boolean, BooleanType())(col("is_active")))
 
df = df.withColumn("reason",
    when(col("product_id").isNull(), lit("Missing product_id"))
    .when(col("price_clean").isNull(), lit("Invalid price"))
    .when(col("email_clean").isNull(), lit("Invalid email"))
    .when(col("created_at_clean").isNull(), lit("Invalid datetime"))
    .otherwise(None)
)
 
# Split valid vs invalid rows
valid_df = df.filter(col("reason").isNull()) \
    .drop("price", "created_at", "email", "is_active") \
    .withColumnRenamed("price_clean", "price") \
    .withColumnRenamed("created_at_clean", "created_at") \
    .withColumnRenamed("email_clean", "email") \
    .withColumnRenamed("is_active_clean", "is_active") \
    .withColumn("created_by", lit("batch_user")) \
    .withColumn("updated_at", lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
 
invalid_df = df.filter(col("reason").isNotNull())
 
# ========================
# Write to Processed + Invalid Folders
# ========================
if valid_df.count() > 0:
    valid_df.write.mode("overwrite").json(processed_path)
 
if invalid_df.count() > 0:
    invalid_df.select("product_id", "price", "email", "created_at", "reason") \
        .write.mode("overwrite").json(invalid_path)
 
# ========================
# Write Metadata Log as JSON
# ========================
log_data = {
    "timestamp": datetime.now().isoformat(),
    "job_name": args['JOB_NAME'],
    "records_total": df.count(),
    "records_valid": valid_df.count(),
    "records_invalid": invalid_df.count(),
    "source_path": failed_path,
    "processed_path": processed_path,
    "invalid_path": invalid_path
}
 
log_json = json.dumps(log_data, indent=4)
log_file = f"/tmp/glue_log_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
 
with open(log_file, "w") as f:
    f.write(log_json)
 
s3 = boto3.client("s3")
s3.upload_file(log_file, bucket_name, f"shopify/logs/{log_file.split('/')[-1]}")
 
# ========================
# Delete files from failed folder
# ========================
failed_prefix = "shopify/failed/"
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=failed_prefix)
if "Contents" in response:
    for obj in response["Contents"]:
        key = obj["Key"]
        # Only delete if the key is not exactly the folder itself
        if key != failed_prefix:
            s3.delete_object(Bucket=bucket_name, Key=key)
            print(f"Deleted: {key}")
 
job.commit()