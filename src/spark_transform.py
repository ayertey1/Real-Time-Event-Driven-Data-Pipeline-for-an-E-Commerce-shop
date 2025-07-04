from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum, avg, countDistinct, count, expr
import boto3
import time
import json
from datetime import datetime
from decimal import Decimal
from pyspark.sql.types import StructType
from logger_utils import logger, log_and_store, log_events

# AWS setup
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb", region_name="eu-north-1")
BUCKET_NAME = "ecommence-datastore"
CATEGORY_TABLE = dynamodb.Table("ecommerce_category_kpis")
ORDER_TABLE = dynamodb.Table("ecommerce_order_kpis")

# Spark setup
spark = SparkSession.builder.appName("KPI-Transformer").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def list_csv_files(prefix):
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)
    keys = []
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                keys.append(obj["Key"])
    return keys


def read_csvs(prefix):
    keys = list_csv_files(prefix)
    if not keys:
        return spark.createDataFrame([], StructType()), []
    dfs = [spark.read.csv(f"s3a://{BUCKET_NAME}/{key}", header=True, inferSchema=True) for key in keys]
    return dfs[0].unionAll(*dfs[1:]) if len(dfs) > 1 else dfs[0], keys


def compute_category_kpis(df):
    df = df.withColumn("order_date", to_date(col("created_at")))
    return df.groupBy("category", "order_date").agg(
        _sum("sale_price").alias("daily_revenue"),
        avg("sale_price").alias("avg_order_value"),
        expr("AVG(CASE WHEN status = 'returned' THEN 1 ELSE 0 END)").alias("return_rate")
    )


def compute_order_kpis(df):
    df = df.withColumn("order_date", to_date(col("created_at")))
    return df.groupBy("order_date").agg(
        countDistinct("order_id").alias("total_orders"),
        _sum("sale_price").alias("total_revenue"),
        count("order_item_id").alias("total_items_sold"),
        expr("AVG(CASE WHEN status = 'returned' THEN 1 ELSE 0 END)").alias("return_rate"),
        countDistinct("user_id").alias("unique_customers")
    )


def write_to_dynamo(df, table, schema_map):
    for row in df.collect():
        item = {k: (Decimal(str(getattr(row, v))) if isinstance(getattr(row, v), float) else getattr(row, v))
                for k, v in schema_map.items()}
        table.put_item(Item=item)


def archive_files(keys):
    for key in keys:
        archive_key = key.replace("raw/", "archive/")
        s3.copy_object(Bucket=BUCKET_NAME, CopySource={"Bucket": BUCKET_NAME, "Key": key}, Key=archive_key)
        s3.delete_object(Bucket=BUCKET_NAME, Key=key)
        log_and_store({"message": "Archived file", "source": key, "destination": archive_key}, level="info")


def write_log_to_s3():
    log_data = "\n".join(json.dumps(event) for event in log_events)
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    log_key = f"logs/transformation/transform_{timestamp}.json"
    s3.put_object(Body=log_data, Bucket=BUCKET_NAME, Key=log_key)


def main():
    max_retries = 3
    wait_seconds = 30
    log_and_store({"message": "Spark transformation script started."}, level="info")

    for attempt in range(1, max_retries + 1):
        logger.info(f"Spark Transformation attempt {attempt}/{max_retries}")

        products_df, products_keys = read_csvs("raw/products/")
        orders_df, orders_keys = read_csvs("raw/orders/")
        order_items_df, order_items_keys = read_csvs("raw/order_items/")

        missing = []
        if products_df.rdd.isEmpty():
            missing.append("products")
        if orders_df.rdd.isEmpty():
            missing.append("orders")
        if order_items_df.rdd.isEmpty():
            missing.append("order_items")

        if not missing:
            break

        log_and_store({
            "message": "One or more datasets missing, retrying...",
            "missing_datasets": missing,
            "attempt": attempt
        }, level="warning")
        write_log_to_s3()

        if attempt < max_retries:
            time.sleep(wait_seconds)
        else:
            log_and_store({
                "message": "Max retries reached, exiting.",
                "missing_datasets": missing
            }, level="error")
            write_log_to_s3()
            return

    merged_orders = order_items_df.join(
        orders_df.select("order_id", "created_at", "status", "user_id"),
        on="order_id"
    ).withColumnRenamed("id", "order_item_id")

    joined_df = merged_orders.join(
        products_df.select("id", "category"),
        merged_orders.product_id == products_df.id
    )

    category_kpis_df = compute_category_kpis(joined_df)
    order_kpis_df = compute_order_kpis(joined_df)

    write_to_dynamo(category_kpis_df, CATEGORY_TABLE, {
        "category": "category",
        "order_date": "order_date",
        "daily_revenue": "daily_revenue",
        "avg_order_value": "avg_order_value",
        "avg_return_rate": "return_rate"
    })

    write_to_dynamo(order_kpis_df, ORDER_TABLE, {
        "order_date": "order_date",
        "summary": "summary",
        "total_orders": "total_orders",
        "total_revenue": "total_revenue",
        "total_items_sold": "total_items_sold",
        "return_rate": "return_rate",
        "unique_customers": "unique_customers"
    })

    archive_files(products_keys + orders_keys + order_items_keys)
    write_log_to_s3()


if __name__ == "__main__":
    main()
