# import json
# import boto3
# import os
# from jsonschema import validate, ValidationError

# s3 = boto3.client('s3')
# glue = boto3.client('glue')

# S3_BUCKET = "my-shopify-data-12345"
# glueJobName = "Real-Time"

# # Load product schema from local file
# with open("product_schema.json") as schema_file:
#     product_schema = json.load(schema_file)


# def validate_product_data(products, schema):
#     if not isinstance(products, list):
#         raise ValidationError("Top-level structure should be a list")

#     valid_products = []

#     for i, product in enumerate(products):
#         try:
#             validate(instance=product, schema=schema)
#             valid_products.append(product)
#         except ValidationError as ve:
#             raise ValidationError(f"Validation error in product index {i}: {ve.message}")

#     return valid_products


# def lambda_handler(event, context):
#     print(f"📦 Listing files in S3 bucket: {S3_BUCKET}/shopify/incoming/")
    
#     try:
#         response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='shopify/incoming/')
#     except Exception as e:
#         print(f"❌ Failed to list objects: {e}")
#         return {"statusCode": 500, "body": "S3 list error"}

#     files = response.get('Contents', [])

#     if not files:
#         print("📂 No files found in incoming folder.")
#         return {"statusCode": 200, "body": "No files in incoming folder."}
#     else:
#         print(f"📁 Found {len(files)} files")

#     for file in files:
#         key = file['Key']
#         if key.endswith('/'):
#             continue  # skip folder markers

#         print(f"🔍 Processing file: {key}")

#         try:
#             obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
#             raw_data = obj['Body'].read()

#             try:
#                 data = json.loads(raw_data)
#             except json.JSONDecodeError as jde:
#                 dest_key = key.replace('incoming/', 'failed/')
#                 print(f"❌ JSON decoding failed: {jde.msg}. Moving to {dest_key}")

#                 # Move to failed and trigger Glue
#                 s3.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': key}, Key=dest_key)
#                 s3.delete_object(Bucket=S3_BUCKET, Key=key)
                
#                 glue.start_job_run(
#                     JobName=glueJobName,
#                     Arguments={'--S3_INPUT_KEY': dest_key}
#                 )
#                 print(f"🧪 Triggered Glue job for {dest_key}")
#                 continue  # go to next file

#             try:
#                 validate_product_data(data, product_schema)
#                 dest_key = key.replace('incoming/', 'processed/')
#                 print(f"✅ Validated. Moving to {dest_key}")
#             except ValidationError as ve:
#                 dest_key = key.replace('incoming/', 'failed/')
#                 print(f"❌ Schema validation failed: {ve.message}. Moving to {dest_key}")

#                 s3.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': key}, Key=dest_key)
#                 s3.delete_object(Bucket=S3_BUCKET, Key=key)
                
#                     try:
                    #     response = glue.start_job_run(JobName=job_name)
                    #     return {
                    #         'statusCode': 200,
                    #         'body': f"Glue job '{job_name}' started successfully with run ID: {response['JobRunId']}"
                    #     }
                    # except Exception as e:
                    #     return {
                    #         'statusCode': 500,
                    #         'body': f"Error starting Glue job: {str(e)}"
                    #     }

#                 continue  # go to next file

#             # Move valid file to processed
#             s3.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': key}, Key=dest_key)
#             s3.delete_object(Bucket=S3_BUCKET, Key=key)

#         except Exception as e:
#             print(f"⚠️ Unexpected error processing file {key}: {e}")

#     print("🎉 Done processing all files.")
#     return {"statusCode": 200, "body": "Validation complete."}

# import boto3

# def lambda_handler(event, context):
#     glue = boto3.client('glue')

#     # Hardcoded Glue job name
#     job_name = "Real-Time"

    # try:
    #     response = glue.start_job_run(JobName=job_name)
    #     return {
    #         'statusCode': 200,
    #         'body': f"Glue job '{job_name}' started successfully with run ID: {response['JobRunId']}"
    #     }
    # except Exception as e:
    #     return {
    #         'statusCode': 500,
    #         'body': f"Error starting Glue job: {str(e)}"
    #     }

# import json
# import boto3
# from jsonschema import validate, ValidationError

# s3 = boto3.client('s3')
# glue = boto3.client('glue')

# S3_BUCKET = "my-shopify-data-12345"
# job_name = "Real-Time"

# # Load product schema from local file
# with open("product_schema.json") as schema_file:
#     product_schema = json.load(schema_file)


# def validate_product_data(products, schema):
#     if not isinstance(products, list):
#         raise ValidationError("Top-level structure should be a list")

#     valid_products = []
#     for i, product in enumerate(products):
#         try:
#             validate(instance=product, schema=schema)
#             valid_products.append(product)
#         except ValidationError as ve:
#             raise ValidationError(f"Validation error in product index {i}: {ve.message}")
#     return valid_products


# def lambda_handler(event, context):
#     print(f"📦 Listing files in S3 bucket: {S3_BUCKET}/shopify/incoming/")
    
#     try:
#         response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='shopify/incoming/')
#     except Exception as e:
#         print(f"❌ Failed to list objects: {e}")
#         return {"statusCode": 500, "body": "S3 list error"}

#     files = response.get('Contents', [])

#     if not files:
#         print("📂 No files found in incoming folder.")
#         return {"statusCode": 200, "body": "No files in incoming folder."}
#     else:
#         print(f"📁 Found {len(files)} files")

#     for file in files:
#         key = file['Key']
#         if key.endswith('/'):
#             continue  # skip folder markers

#         print(f"🔍 Processing file: {key}")

#         try:
#             obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
#             raw_data = obj['Body'].read()

#             try:
#                 data = json.loads(raw_data)
#             except json.JSONDecodeError as jde:
#                 dest_key = key.replace('incoming/', 'failed/')
#                 print(f"❌ JSON decoding failed: {jde.msg}. Moving to {dest_key}")

#                 s3.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': key}, Key=dest_key)
#                 s3.delete_object(Bucket=S3_BUCKET, Key=key)

               
#                 continue  # process next file

#             try:
#                 validate_product_data(data, product_schema)
#                 dest_key = key.replace('incoming/', 'processed/')
#                 print(f"✅ Validated. Moving to {dest_key}")
#             except ValidationError as ve:
#                 dest_key = key.replace('incoming/', 'failed/')
#                 print(f"❌ Schema validation failed: {ve.message}. Moving to {dest_key}")

#                 s3.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': key}, Key=dest_key)
#                 s3.delete_object(Bucket=S3_BUCKET, Key=key)

           
#                 continue  # process next file

#             # If valid, move to processed
#             s3.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': key}, Key=dest_key)
#             s3.delete_object(Bucket=S3_BUCKET, Key=key)

#         except Exception as e:
#             print(f"⚠️ Unexpected error processing file {key}: {e}")
    
#     try:
#         response = glue.start_job_run(JobName=job_name)
#         return {
#             'statusCode': 200,
#             'body': f"Glue job '{job_name}' started successfully with run ID: {response['JobRunId']}"
#         }
#     except Exception as e:
#         return {
#             'statusCode': 500,
#             'body': f"Error starting Glue job: {str(e)}"
#         }

#     print("🎉 Done processing all files.")
#     return {"statusCode": 200, "body": "Validation complete."}
import json
import boto3
from jsonschema import validate, ValidationError

s3 = boto3.client('s3')
glue = boto3.client('glue')

S3_BUCKET = "my-shopify-data-12345"

# Glue job names based on prefix
glue_job_map = {
    "product": "Product-Real-Time",    # <-- replace with actual job names
    "location": "Location-Real-Time",
    "customer": "Custom-Collection-Real-Time"
}

# Load product schema from local file
with open("product_schema.json") as schema_file:
    product_schema = json.load(schema_file)


def validate_product_data(products, schema):
    if not isinstance(products, list):
        raise ValidationError("Top-level structure should be a list")

    valid_products = []
    for i, product in enumerate(products):
        try:
            validate(instance=product, schema=schema)
            valid_products.append(product)
        except ValidationError as ve:
            raise ValidationError(f"Validation error in product index {i}: {ve.message}")
    return valid_products


def lambda_handler(event, context):
    print(f"📦 Listing files in S3 bucket: {S3_BUCKET}/shopify/incoming/")
    
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='shopify/incoming/')
    except Exception as e:
        print(f"❌ Failed to list objects: {e}")
        return {"statusCode": 500, "body": "S3 list error"}

    files = response.get('Contents', [])
    if not files:
        print("📂 No files found in incoming folder.")
        return {"statusCode": 200, "body": "No files in incoming folder."}
    else:
        print(f"📁 Found {len(files)} files")
    for file in files:
        key = file['Key']
        if key.endswith('/'):
            continue  # skip folder markers

        print(f"🔍 Processing file: {key}")

        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            raw_data = obj['Body'].read()

            try:
                data = json.loads(raw_data)
            except json.JSONDecodeError as jde:
                dest_key = key.replace('incoming/', 'failed/')
                print(f"❌ JSON decoding failed: {jde.msg}. Moving to {dest_key}")

                s3.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': key}, Key=dest_key)
                s3.delete_object(Bucket=S3_BUCKET, Key=key)

               
                continue  # process next file

            try:
                validate_product_data(data, product_schema)
                dest_key = key.replace('incoming/', 'processed/')
                print(f"✅ Validated. Moving to {dest_key}")
            except ValidationError as ve:
                dest_key = key.replace('incoming/', 'failed/')
                print(f"❌ Schema validation failed: {ve.message}. Moving to {dest_key}")

                s3.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': key}, Key=dest_key)
                s3.delete_object(Bucket=S3_BUCKET, Key=key)

           
                continue  # process next file

            # If valid, move to processed
            s3.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': key}, Key=dest_key)
            s3.delete_object(Bucket=S3_BUCKET, Key=key)

        except Exception as e:
            print(f"⚠️ Unexpected error processing file {key}: {e}")


    #accessing files from failed folder

    print(f"📦 Listing files in S3 bucket: {S3_BUCKET}/shopify/failed/")
    
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='shopify/failed/')
    except Exception as e:
        print(f"❌ Failed to list objects: {e}")
        return {"statusCode": 500, "body": "S3 list error"}

    files_failed = response.get('Contents', [])
    if not files_failed:
        print("📂 No files found in failed folder.")
        return {"statusCode": 200, "body": "No files in failed folder."}
    else:
        print(f"📁 Found {len(files_failed)} files")

    # Track which jobs to trigger
    job_triggers = {
        "product": False,
        "location": False,
        "customer": False
    }

    for file in files_failed:
        key = file['Key']
        if key.endswith('/'):
            continue  # skip folder markers

        print(f"🔍 Processing file: {key}")

        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            raw_data = obj['Body'].read()

            try:
                data = json.loads(raw_data)
            except json.JSONDecodeError as jde:
                dest_key = key.replace('failed/', 'failed-again/')
                print(f"❌ JSON decoding failed: {jde.msg}. Moving to {dest_key}")
                s3.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': key}, Key=dest_key)
                s3.delete_object(Bucket=S3_BUCKET, Key=key)
                continue

            # try:
            #     validate_product_data(data, product_schema)
            #     dest_key = key.replace('failed/', 'processed/')
            #     print(f"✅ Validated. Moving to {dest_key}")
            # except ValidationError as ve:
            #     dest_key = key.replace('failed/', 'failed-again/')
            #     print(f"❌ Schema validation failed: {ve.message}. Moving to {dest_key}")
            #     s3.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': key}, Key=dest_key)
            #     s3.delete_object(Bucket=S3_BUCKET, Key=key)
            #     continue

            # # Move to processed
            # s3.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': key}, Key=dest_key)
            s3.delete_object(Bucket=S3_BUCKET, Key=key)

            # Determine job type based on filename prefix
            if 'product' in key.lower():
                job_triggers["product"] = True
            elif 'location' in key.lower():
                job_triggers["location"] = True
            elif 'custom_collections' in key.lower():
                job_triggers["customer"] = True

        except Exception as e:
            print(f"⚠️ Unexpected error processing file {key}: {e}")

    # Trigger Glue jobs as needed
    started_jobs = []

    for job_type, should_trigger in job_triggers.items():
        if should_trigger:
            try:
                job_name = glue_job_map[job_type]
                response = glue.start_job_run(JobName=job_name)
                started_jobs.append(f"{job_type}: {response['JobRunId']}")
                print(f"🚀 Started {job_type} Glue job: {response['JobRunId']}")
            except Exception as e:
                print(f"❌ Error starting Glue job for {job_type}: {e}")

    if started_jobs:
        return {
            'statusCode': 200,
            'body': f"Glue jobs started: {', '.join(started_jobs)}"
        }
    else:
        return {
            'statusCode': 200,
            'body': "No Glue jobs were started. No valid file types found."
        }
