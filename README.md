📥 Reads from:

Prefix='shopify/incoming/'
It looks in:


s3://<your S3_BUCKET>/shopify/incoming/
It pulls JSON files from there to validate.

✅ If valid, moves to:

dest_key = key.replace('incoming/', 'processed/')
It moves to:


s3://<your S3_BUCKET>/shopify/processed/
❌ If invalid, moves to:

dest_key = key.replace('incoming/', 'failed/')
It moves to:

s3://<your S3_BUCKET>/shopify/failed/
# AWS_Ingestion
