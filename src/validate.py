import pandas as pd
import boto3
import io
import logging
from botocore.exceptions import ClientError
from pythonjsonlogger import jsonlogger

# Logging setup
logger = logging.getLogger()
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

BUCKET_NAME = "ecommence-datastore"

def read_csv_from_s3(key):
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))
        logger.info({"message": f"Loaded {key}", "rows": len(df)})
        return df
    except ClientError as e:
        logger.error({"error": str(e)})
        raise

def validate_schema(df, expected_cols, dataset_name):
    actual = set(df.columns)
    expected = set(expected_cols)
    if actual != expected:
        missing = expected - actual
        extra = actual - expected
        logger.error({
            "dataset": dataset_name,
            "missing_columns": list(missing),
            "extra_columns": list(extra)
        })
        raise ValueError(f"Schema mismatch in {dataset_name}")

def main():
    products = read_csv_from_s3("staging/products/products.csv")
    orders = read_csv_from_s3("staging/orders/orders_part1.csv")
    order_items = read_csv_from_s3("staging/order_items/order_items_part1.csv")

    validate_schema(products, [
        "id","sku","cost","category","name","brand","retail_price","department"
    ], "products")
    validate_schema(orders, [
        "order_id","user_id","status","created_at","returned_at","shipped_at","delivered_at","num_of_item"
    ], "orders")
    validate_schema(order_items, [
        "id","order_id","user_id","product_id","status","created_at","shipped_at","delivered_at","returned_at","sale_price"
    ], "order_items")

    # Referential Integrity
    missing_orders = order_items[~order_items["order_id"].isin(orders["order_id"])]
    if not missing_orders.empty:
        logger.error({"message": "Order items with invalid order_id", "count": len(missing_orders)})
        raise ValueError("Invalid order_ids found in order_items")

    missing_products = order_items[~order_items["product_id"].isin(products["id"])]
    if not missing_products.empty:
        logger.error({"message": "Order items with invalid product_id", "count": len(missing_products)})
        raise ValueError("Invalid product_ids found in order_items")

    logger.info({"message": "Validation passed"})

if __name__ == "__main__":
    main()
