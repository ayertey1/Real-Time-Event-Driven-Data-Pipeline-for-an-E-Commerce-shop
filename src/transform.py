import pandas as pd
import boto3
import io
import os
import json
from datetime import datetime
import time
from decimal import Decimal
from botocore.exceptions import ClientError

from logger_utils import logger, log_and_store, log_events

# AWS Clients
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb", region_name="eu-north-1")

BUCKET_NAME = "ecommence-datastore"

# DynamoDB tables
CATEGORY_TABLE = dynamodb.Table("ecommerce_category_kpis")
ORDER_TABLE = dynamodb.Table("ecommerce_order_kpis")


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


def read_all_csvs_in_prefix(prefix):
    keys = list_csv_files(prefix)
    dfs = []
    for key in keys:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))
        dfs.append(df)
    if dfs:
        return pd.concat(dfs, ignore_index=True), keys
    else:
        return pd.DataFrame(), []


def compute_category_kpis(joined_df):
    """
    Compute category-level KPIs per day.
    """
    joined_df["order_date"] = pd.to_datetime(joined_df["created_at"]).dt.date.astype(str)

    kpis = (
        joined_df.groupby(["category", "order_date"])
        .agg(
            daily_revenue=("sale_price", "sum"),
            avg_order_value=("sale_price", "mean"),
            return_rate=("status", lambda x: (x == "returned").mean())
        )
        .reset_index()
    )
    return kpis


def compute_order_kpis(joined_df):
    """
    Compute order-level KPIs per day.
    """
    joined_df["order_date"] = pd.to_datetime(joined_df["created_at"]).dt.date.astype(str)

    daily = (
        joined_df.groupby("order_date")
        .agg(
            total_orders=("order_id", pd.Series.nunique),
            total_revenue=("sale_price", "sum"),
            total_items_sold=("order_item_id", "count"),
            return_rate=("status", lambda x: (x == "returned").mean()),
            unique_customers=("user_id", pd.Series.nunique)
        )
        .reset_index()
    )
    return daily


def write_category_kpis_to_dynamo(kpis_df):
    for _, row in kpis_df.iterrows():
        item = {
            "category": row["category"],
            "order_date": row["order_date"],
            "daily_revenue": Decimal(str(row["daily_revenue"])),
            "avg_order_value": Decimal(str(row["avg_order_value"])),
            "avg_return_rate": Decimal(str(round(row["return_rate"], 4)))
        }
        CATEGORY_TABLE.put_item(Item=item)


def write_order_kpis_to_dynamo(kpis_df):
    for _, row in kpis_df.iterrows():
        item = {
            "order_date": row["order_date"],
            "summary": "daily_summary",
            "total_orders": int(row["total_orders"]),
            "total_revenue": Decimal(str(row["total_revenue"])),
            "total_items_sold": int(row["total_items_sold"]),
            "return_rate": Decimal(str(round(row["return_rate"], 4))),
            "unique_customers": int(row["unique_customers"])
        }
        ORDER_TABLE.put_item(Item=item)


def archive_files(keys):
    for key in keys:
        archive_key = key.replace("raw/", "archive/")
        s3.copy_object(Bucket=BUCKET_NAME, CopySource={'Bucket': BUCKET_NAME, 'Key': key}, Key=archive_key)
        s3.delete_object(Bucket=BUCKET_NAME, Key=key)
        log_and_store({
            "message": "Archived file",
            "source": key,
            "destination": archive_key
        }, level="info")


def write_log_to_s3():
    log_data = "\n".join(json.dumps(event) for event in log_events)
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    log_key = f"logs/transformation/transform_{timestamp}.json"
    s3.put_object(Body=log_data, Bucket=BUCKET_NAME, Key=log_key)


def main():
    max_retries = 3
    wait_seconds = 30
    log_and_store({"message": "Transformation script started."}, level="info")


    for attempt in range(1, max_retries + 1):
        logger.info(f"Transformation attempt {attempt}/{max_retries}")
        products_df, products_keys = read_all_csvs_in_prefix("raw/products/")
        orders_df, orders_keys = read_all_csvs_in_prefix("raw/orders/")
        order_items_df, order_items_keys = read_all_csvs_in_prefix("raw/order_items/")

        missing_datasets = []

        if order_items_df.empty:
            missing_datasets.append("order_items")
        if orders_df.empty:
            missing_datasets.append("orders")
        if products_df.empty:
            missing_datasets.append("products")

        if not missing_datasets:
            break  # Exit retry loop if all datasets are available

        # Log missing and wait
        log_and_store({
            "message": "One or more required datasets are empty. Waiting before retry.",
            "missing_datasets": missing_datasets,
            "attempt": attempt
        }, level="warning")
        write_log_to_s3()

        if attempt < max_retries:
            time.sleep(wait_seconds)
        else:
            log_and_store({
                "message": "Max retries reached. Datasets still missing. Exiting transformation.",
                "missing_datasets": missing_datasets
            }, level="error")
            write_log_to_s3()
            return

    # Merge datasets
    merged_orders = order_items_df.merge(
        orders_df[["order_id", "created_at", "status", "user_id"]],
        on="order_id",
        suffixes=("", "_order")
    )
    merged_orders = merged_orders.rename(columns={"id": "order_item_id"})

    joined_df = merged_orders.merge(
        products_df[["id", "category"]],
        left_on="product_id",
        right_on="id",
        suffixes=("", "_product")
    )

    # Compute KPIs
    category_kpis = compute_category_kpis(joined_df)
    order_kpis = compute_order_kpis(joined_df)

    # Write to DynamoDB
    write_category_kpis_to_dynamo(category_kpis)
    write_order_kpis_to_dynamo(order_kpis)

    # Archive processed files
    all_processed_keys = products_keys + orders_keys + order_items_keys
    archive_files(all_processed_keys)

    write_log_to_s3()

if __name__ == "__main__":
    main()
