import os
import sys
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()

import boto3

from botocore.config import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    avg,
    min as spark_min,
    max as spark_max,
    to_date,
    current_timestamp
)


# =====================================================
# CONFIG
# =====================================================

BUCKET_NAME = "enterprise-data-lake"

S3_ENDPOINT = os.getenv(
    "S3_ENDPOINT",
    "http://localhost:9000"
)
S3_ACCESS_KEY = os.getenv("RUSTFS_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("RUSTFS_SECRET_KEY")

os.environ["PYSPARK_PYTHON"] = sys.executable


# =====================================================
# RUSTFS CLIENT
# =====================================================

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name="us-east-1",
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"}
    )
)


# =====================================================
# SPARK
# =====================================================

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("SilverToGold")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# =====================================================
# RUN METADATA
# =====================================================

now = datetime.now(timezone.utc)

processing_date = now.strftime("%Y-%m-%d")
run_timestamp = now.strftime("%Y%m%d_%H%M%S")


# =====================================================
# GET LATEST SILVER FILE
# =====================================================

def get_latest_object(prefix):

    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=prefix
    )

    objects = [
        obj
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    if not objects:
        raise RuntimeError(
            f"No Silver files found: {prefix}"
        )

    latest = max(
        objects,
        key=lambda x: x["LastModified"]
    )

    return latest["Key"]


# =====================================================
# DOWNLOAD
# =====================================================

def download(key, path):

    print(f"Downloading: {key}")

    s3.download_file(
        BUCKET_NAME,
        key,
        str(path)
    )


# =====================================================
# WRITE GOLD
# =====================================================

def upload_gold(dataframe, dataset_name, temp_dir):

    print(f"Writing {dataset_name}...")

    pandas_df = dataframe.toPandas()

    output_file = (
        Path(temp_dir)
        / f"{dataset_name}_{run_timestamp}.parquet"
    )

    pandas_df.to_parquet(
    output_file,
    engine="pyarrow",
    index=False,
    coerce_timestamps="us",
    allow_truncated_timestamps=True
)

    object_key = (
        f"gold/{dataset_name}/"
        f"processing_date={processing_date}/"
        f"{dataset_name}_{run_timestamp}.parquet"
    )

    s3.upload_file(
        str(output_file),
        BUCKET_NAME,
        object_key
    )

    print(
        f"Uploaded -> s3://{BUCKET_NAME}/{object_key}"
    )


# =====================================================
# MAIN
# =====================================================

with tempfile.TemporaryDirectory() as temp_dir:

    temp = Path(temp_dir)

    # -------------------------------------------------
    # FIND SILVER FILES
    # -------------------------------------------------

    customers_key = get_latest_object(
        "silver/customers/"
    )

    orders_key = get_latest_object(
        "silver/orders/"
    )

    items_key = get_latest_object(
        "silver/order_items/"
    )

    products_key = get_latest_object(
        "silver/products/"
    )


    # -------------------------------------------------
    # DOWNLOAD
    # -------------------------------------------------

    customers_file = temp / "customers.parquet"
    orders_file = temp / "orders.parquet"
    items_file = temp / "order_items.parquet"
    products_file = temp / "products.parquet"

    download(customers_key, customers_file)
    download(orders_key, orders_file)
    download(items_key, items_file)
    download(products_key, products_file)


    # -------------------------------------------------
    # READ SILVER
    # -------------------------------------------------

    customers = spark.read.parquet(
        str(customers_file)
    )

    orders = spark.read.parquet(
        str(orders_file)
    )

    items = spark.read.parquet(
        str(items_file)
    )

    products = spark.read.parquet(
        str(products_file)
    )


    # =================================================
    # VALID SALES ONLY
    # =================================================

    valid_orders = orders.filter(
        col("status") != "CANCELLED"
    )


    # =================================================
    # GOLD 1 - DAILY SALES
    # =================================================

    daily_sales = (
        valid_orders
        .withColumn(
            "sales_date",
            to_date("order_timestamp")
        )
        .groupBy("sales_date")
        .agg(
            count("order_id")
                .alias("total_orders"),

            countDistinct("customer_id")
                .alias("unique_customers"),

            spark_sum("total_amount")
                .alias("total_revenue"),

            avg("total_amount")
                .alias("avg_order_value")
        )
        .withColumn(
            "_gold_processed_at",
            current_timestamp()
        )
    )


    # =================================================
    # GOLD 2 - CUSTOMER 360
    # =================================================

    customer_metrics = (
        valid_orders
        .groupBy("customer_id")
        .agg(
            count("order_id")
                .alias("total_orders"),

            spark_sum("total_amount")
                .alias("total_spent"),

            avg("total_amount")
                .alias("avg_order_value"),

            spark_min("order_timestamp")
                .alias("first_order_at"),

            spark_max("order_timestamp")
                .alias("last_order_at")
        )
    )

    customer_360 = (
        customers
        .join(
            customer_metrics,
            on="customer_id",
            how="left"
        )
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "country",
            "created_at",
            "total_orders",
            "total_spent",
            "avg_order_value",
            "first_order_at",
            "last_order_at"
        )
        .fillna({
            "total_orders": 0,
            "total_spent": 0,
            "avg_order_value": 0
        })
        .withColumn(
            "_gold_processed_at",
            current_timestamp()
        )
    )


    # =================================================
    # GOLD 3 - PRODUCT PERFORMANCE
    # =================================================

    valid_items = (
        items
        .join(
            valid_orders.select(
                "order_id"
            ),
            on="order_id",
            how="inner"
        )
    )

    product_metrics = (
        valid_items
        .withColumn(
            "line_revenue",
            col("quantity") * col("unit_price")
        )
        .groupBy("product_id")
        .agg(
            countDistinct("order_id")
                .alias("total_orders"),

            spark_sum("quantity")
                .alias("units_sold"),

            spark_sum("line_revenue")
                .alias("total_revenue")
        )
    )

    product_performance = (
        products
        .join(
            product_metrics,
            on="product_id",
            how="left"
        )
        .select(
            "product_id",
            "product_name",
            "category",
            "unit_price",
            "stock",
            "rating",
            "total_orders",
            "units_sold",
            "total_revenue"
        )
        .fillna({
            "total_orders": 0,
            "units_sold": 0,
            "total_revenue": 0
        })
        .withColumn(
            "_gold_processed_at",
            current_timestamp()
        )
    )


    # =================================================
    # CHECK RESULTS
    # =================================================

    print()
    print("======================================")
    print("GOLD DATASETS")
    print("======================================")

    print(
        f"daily_sales         : "
        f"{daily_sales.count():,}"
    )

    print(
        f"customer_360        : "
        f"{customer_360.count():,}"
    )

    print(
        f"product_performance : "
        f"{product_performance.count():,}"
    )


    # =================================================
    # WRITE GOLD
    # =================================================

    upload_gold(
        daily_sales,
        "daily_sales",
        temp_dir
    )

    upload_gold(
        customer_360,
        "customer_360",
        temp_dir
    )

    upload_gold(
        product_performance,
        "product_performance",
        temp_dir
    )


spark.stop()

print()
print("======================================")
print("SILVER -> GOLD COMPLETE")
print("======================================")