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

# Dataset schema definitions
DATASETS = {
    "products": {
        "key": "staging/products/products.csv",
        "expected_columns": ["id", "sku", "cost", "category", "name", "brand", "retail_price", "department"]
    },
    "orders": {
        "key": "staging/orders/orders_part1.csv",
        "expected_columns": ["order_id", "user_id", "status", "created_at", "returned_at", "shipped_at", "delivered_at", "num_of_item"]
    },
    "order_items": {
        "key": "staging/order_items/order_items_part1.csv",
        "expected_columns": ["id", "order_id", "user_id", "product_id", "status", "created_at", "shipped_at", "delivered_at", "returned_at", "sale_price"]
    }
}

# Logging setup
logger = logging.getLogger()
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

log_events = []

def log_and_store(event, level="info"):
    log_events.append(event)
    getattr(logger, level)(event)

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
    source_key = config["key"]
    expected = set(config["expected_columns"])

    try:
        actual = set(read_csv_header_from_s3(source_key))
        if actual != expected:
            missing = expected - actual
            extra = actual - expected
            log_and_store({
                "dataset": dataset_name,
                "status": "rejected",
                "missing_columns": list(missing),
                "extra_columns": list(extra)
            }, level="error")
            target_key = f"rejected/{dataset_name}/{os.path.basename(source_key)}"
        else:
            log_and_store({
                "dataset": dataset_name,
                "status": "validated"
            }, level="info")
            target_key = f"raw/{dataset_name}/{os.path.basename(source_key)}"

        move_file(source_key, target_key)

    except Exception as e:
        log_and_store({
            "dataset": dataset_name,
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
