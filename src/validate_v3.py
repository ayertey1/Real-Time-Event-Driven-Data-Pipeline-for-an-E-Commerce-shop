import pandas as pd
import boto3
import io
import logging
import os
import json
from datetime import datetime
from botocore.exceptions import ClientError
from pythonjsonlogger import jsonlogger

# AWS setup
s3 = boto3.client("s3")
BUCKET_NAME = "ecommence-datastore"

# Dataset schema definitions with prefixes
DATASETS = {
    "products": {
        "prefix": "staging/products/",
        "expected_columns": ["id", "sku", "cost", "category", "name", "brand", "retail_price", "department"]
    },
    "orders": {
        "prefix": "staging/orders/",
        "expected_columns": ["order_id", "user_id", "status", "created_at", "returned_at", "shipped_at", "delivered_at", "num_of_item"]
    },
    "order_items": {
        "prefix": "staging/order_items/",
        "expected_columns": ["id", "order_id", "user_id", "product_id", "status", "created_at", "shipped_at", "delivered_at", "returned_at", "sale_price"]
    }
}

# Logging setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if logger.hasHandlers():
    logger.handlers.clear()
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

log_events = []

def log_and_store(event, level="info"):
    log_events.append(event)
    getattr(logger, level)(event)

def list_csv_files(prefix):
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)

    keys = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv"):
                keys.append(key)
    return keys

def read_csv_header_from_s3(key):
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()), nrows=0)
        return df.columns.tolist()
    except Exception as e:
        raise RuntimeError(f"Failed to read {key}: {e}")

def move_file(source_key, target_key):
    copy_source = {'Bucket': BUCKET_NAME, 'Key': source_key}
    s3.copy_object(Bucket=BUCKET_NAME, CopySource=copy_source, Key=target_key)
    s3.delete_object(Bucket=BUCKET_NAME, Key=source_key)

def validate_and_route(dataset_name, config):
    expected = set(config["expected_columns"])
    keys = list_csv_files(config["prefix"])

    for key in keys:
        try:
            actual = set(read_csv_header_from_s3(key))
            if actual != expected:
                missing = expected - actual
                extra = actual - expected
                log_and_store({
                    "dataset": dataset_name,
                    "file": key,
                    "status": "rejected",
                    "missing_columns": list(missing),
                    "extra_columns": list(extra)
                }, level="error")
                target_key = f"rejected/{dataset_name}/{os.path.basename(key)}"
            else:
                log_and_store({
                    "dataset": dataset_name,
                    "file": key,
                    "status": "validated"
                }, level="info")
                target_key = f"raw/{dataset_name}/{os.path.basename(key)}"

            move_file(key, target_key)

        except Exception as e:
            log_and_store({
                "dataset": dataset_name,
                "file": key,
                "status": "error",
                "error": str(e)
            }, level="error")

def write_log_to_s3():
    log_data = "\n".join(json.dumps(event) for event in log_events)
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    log_key = f"logs/validation/validate_{timestamp}.json"
    s3.put_object(Body=log_data, Bucket=BUCKET_NAME, Key=log_key)

def main():
    for dataset, config in DATASETS.items():
        validate_and_route(dataset, config)
    write_log_to_s3()

if __name__ == "__main__":
    main()
