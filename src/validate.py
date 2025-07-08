import pandas as pd
import boto3
import io
import os
import json
import time
from datetime import datetime
from botocore.exceptions import ClientError

from logger_utils import logger, log_and_store, log_events

# AWS S3 Client
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


def list_csv_files(prefix):
    """
    List all .csv files under a given S3 prefix.
    """
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
    """
    Read only the header of a CSV file from S3.
    """
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()), nrows=0)
    return df.columns.tolist()


def move_file(source_key, target_key):
    """
    Move file within S3 (copy + delete).
    """
    copy_source = {'Bucket': BUCKET_NAME, 'Key': source_key}
    s3.copy_object(Bucket=BUCKET_NAME, CopySource=copy_source, Key=target_key)
    s3.delete_object(Bucket=BUCKET_NAME, Key=source_key)


def validate_and_collect_moves(dataset_name, config):
    """
    Validate schema of all files in staging prefix.
    Return a list of (source_key, target_key) pairs for batch moving.
    """
    expected = set(config["expected_columns"])
    keys = list_csv_files(config["prefix"])

    if not keys:
        log_and_store({
            "dataset": dataset_name,
            "status": "no_files_found"
        }, level="info")
        return [], False

    moves = []

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

            moves.append((key, target_key))

        except Exception as e:
            log_and_store({
                "dataset": dataset_name,
                "file": key,
                "status": "error",
                "error": str(e)
            }, level="error")

    return moves, True


def batch_move_files(moves):
    """
    Move all files in the provided list of (source, target) tuples.
    """
    for source, target in moves:
        move_file(source, target)
        log_and_store({
            "message": "Moved file",
            "source": source,
            "target": target
        }, level="info")


def read_all_csvs_in_prefix(prefix):
    """
    Read and concatenate all CSV files under a prefix.
    """
    keys = list_csv_files(prefix)
    dfs = []
    for key in keys:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))
        dfs.append(df)
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()


def validate_referential_integrity():
    """
    Checks referential integrity between datasets:
      - order_items.order_id exists in orders.order_id
      - order_items.product_id exists in products.id
    """
    logger.info("Starting referential integrity validation...")

    orders_df = read_all_csvs_in_prefix("raw/orders/")
    products_df = read_all_csvs_in_prefix("raw/products/")
    order_items_df = read_all_csvs_in_prefix("raw/order_items/")

    if order_items_df.empty or orders_df.empty or products_df.empty:
        logger.warning("One or more datasets empty. Skipping referential integrity checks.")
        return

    # Check order_id references
    missing_orders = order_items_df.loc[
        ~order_items_df["order_id"].isin(orders_df["order_id"])
    ]
    if not missing_orders.empty:
        log_and_store({
            "dataset": "order_items",
            "status": "invalid_references",
            "reference": "orders",
            "invalid_count": len(missing_orders)
        }, level="error")

    # Check product_id references
    missing_products = order_items_df.loc[
        ~order_items_df["product_id"].isin(products_df["id"])
    ]
    if not missing_products.empty:
        log_and_store({
            "dataset": "order_items",
            "status": "invalid_references",
            "reference": "products",
            "invalid_count": len(missing_products)
        }, level="error")


def write_log_to_s3():
    """
    Write accumulated log events to S3 in JSONL format.
    """
    log_data = "\n".join(json.dumps(event) for event in log_events)
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    log_key = f"logs/validation/validate_{timestamp}.json"
    s3.put_object(Body=log_data, Bucket=BUCKET_NAME, Key=log_key)


def main():
    max_retries = 3
    wait_seconds = 30

    for attempt in range(max_retries):
        log_events.clear()
        logger.info(f"Validation attempt {attempt + 1}/{max_retries}")
        any_files_found = False
        all_moves = []

        for dataset, config in DATASETS.items():
            moves, found = validate_and_collect_moves(dataset, config)
            all_moves.extend(moves)
            any_files_found = any_files_found or found

        if any_files_found:
            # Move files after all validations
            batch_move_files(all_moves)
            # Referential integrity check
            validate_referential_integrity()
            # Write logs
            write_log_to_s3()
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key="validation/result.json",
                Body=json.dumps({"validated": True})
            )
            return

        log_and_store({
            "message": "No files found in staging area on this attempt",
            "timestamp": datetime.utcnow().isoformat()
        }, level="info")
        write_log_to_s3()
        time.sleep(wait_seconds)

    logger.info("Max retries reached. Exiting without processing any files.")
    s3.put_object(
    Bucket=BUCKET_NAME,
    Key="validation/result.json",
    Body=json.dumps({"validated": False})
)



if __name__ == "__main__":
    main()
