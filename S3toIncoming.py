import json
import boto3
import requests
import os
from datetime import datetime

# Shopify credentials from environment variables (set in Lambda console)
SHOP_NAME = os.environ['SHOP_NAME']
API_KEY = os.environ['API_KEY']
PASSWORD = os.environ['PASSWORD']
S3_BUCKET = os.environ['S3_BUCKET']  # should be "my-shopify-data-12345"

# Shopify endpoints to fetch
DATA_TYPES = {
    "products": "products",
    "orders": "orders",
    "customers": "customers",
    "custom_collections": "custom_collections",
    "smart_collections": "smart_collections",
    "locations": "locations"
}

def fetch_all_pages(url):
    """Fetch all pages from a paginated Shopify endpoint"""
    all_data = []
    while url:
        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()
        key = list(json_data.keys())[0]
        data = json_data.get(key, [])
        all_data.extend(data)

        # Handle pagination using Link header
        link_header = response.headers.get('Link', '')
        if 'rel="next"' in link_header:
            parts = link_header.split(',')
            next_url = None
            for part in parts:
                if 'rel="next"' in part:
                    next_url = part.split(';')[0].strip()[1:-1]
                    break
            url = next_url
        else:
            url = None
    return all_data

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')
    s3_folder = "shopify/incoming"

    for data_type, endpoint in DATA_TYPES.items():
        print(f"Fetching {data_type}...")

        base_url = f"https://{API_KEY}:{PASSWORD}@{SHOP_NAME}.myshopify.com/admin/api/2023-04/{endpoint}.json?limit=250"
        
        try:
            data = fetch_all_pages(base_url)
            if not data:
                print(f"No {data_type} found.")
                continue

            filename = f"{data_type}_{timestamp}.json"
            s3_key = f"{s3_folder}/{filename}"

            print(f"Uploading to: s3://{S3_BUCKET}/{s3_key}")
            s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=json.dumps(data))

            print(f"✅ Uploaded {len(data)} {data_type} records to S3: {s3_key}")

        except Exception as e:
            print(f"❌ Error fetching {data_type}: {e}")
    
    return {
        "statusCode": 200,
        "body": f"Shopify data fetched and stored under {s3_folder}/ at {timestamp}"
    }
