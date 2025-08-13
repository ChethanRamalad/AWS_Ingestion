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
from pyspark.sql.functions import col, lit, udf, when, explode
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


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
base_path = f"s3://{bucket_name}/shopify"

# ========================
# Utility Functions
# ========================
def validate_datetime(input_str):
    try:
        if input_str is None or not isinstance(input_str, str):
            return None
        cleaned_str = re.sub(r'[-,]', ' ', input_str.strip())
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
        return format(price_float, 'f').rstrip('0').rstrip('.')
    except:
        return None

normalize_price_udf = udf(normalize_price_to_inr, StringType())

# ========================
# Process Each File Type
# ========================
def process_customer(file):
    df = spark.read.option("multiLine", True).json(file)
    # Apply datetime transformation to required fields
    validate_datetime_udf = udf(validate_datetime, StringType())
    for dt_col in ["updated_at", "published_at"]:
        if dt_col in df.columns:
            df = df.withColumn(dt_col, validate_datetime_udf(col(dt_col)))
            
    window_spec = Window.partitionBy("id").orderBy("title")
    df = df.withColumn("row_num", row_number().over(window_spec))

    df = df.withColumn("reason",
        when(col("id").isNull(), "Missing id")
        .when(col("title").isNull(), "Missing title")
        .when(col("handle").isNull(), "Missing handle")
        .when(col("published_scope").isNull(), "Missing published_scope")
        .when(col("row_num") > 1, "Duplicate customer id")
        .otherwise(None)
    )

    valid_df = df.filter(col("reason").isNull()) \
                .withColumn("created_by", lit("batch_user")) \
                .withColumn("updated_at", lit(datetime.now().strftime('%d/%m/%Y %H:%M')))

    invalid_df = df.filter(col("reason").isNotNull()).select("id", "title", "handle", "published_scope", "reason")
    return valid_df, invalid_df

# ========================
# Iterate Over Files in Failed Folder
# ========================
s3 = boto3.client("s3")
prefix = "shopify/failed/"
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

if "Contents" in response:
    for obj in response["Contents"]:
        key = obj["Key"]
        if key.endswith(".json"):
            file_path = f"s3://{bucket_name}/{key}"
            
            if key.startswith("shopify/failed/custom_collections"):
                entity = "customer"
                valid_df, invalid_df = process_customer(file_path)
            else:
                continue

            processed_path = f"{base_path}/processed/{entity}/"
            invalid_path = f"{base_path}/invalid/{entity}/"

            if valid_df.count() > 0:
                valid_df.write.mode("overwrite").json(processed_path)

            if invalid_df.count() > 0:
                invalid_df.write.mode("overwrite").json(invalid_path)

            reason_counts = invalid_df.groupBy("reason").count().collect()
            reason_summary = {row["reason"]: row["count"] for row in reason_counts}

            log_data = {
                "timestamp": datetime.now().isoformat(),
                "job_name": args['JOB_NAME'],
                "entity": entity,
                "records_total": valid_df.count() + invalid_df.count(),
                "records_valid": valid_df.count(),
                "records_invalid": invalid_df.count(),
                "invalid_reasons_breakdown": reason_summary,
                "source_path": file_path,
                "processed_path": processed_path,
                "invalid_path": invalid_path
            }

            log_file = f"/tmp/glue_log_{entity}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
            with open(log_file, "w") as f:
                f.write(json.dumps(log_data, indent=4))

            s3.upload_file(log_file, bucket_name, f"shopify/logs/{log_file.split('/')[-1]}")
            s3.delete_object(Bucket=bucket_name, Key=key)
            print(f"Deleted processed file: {key}")

job.commit()
