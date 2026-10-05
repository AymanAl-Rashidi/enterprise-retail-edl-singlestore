import os
import sys
import tempfile
from pathlib import Path
from datetime import datetime, timezone

import boto3

from botocore.config import Config

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    upper,
    current_timestamp,
    explode
)


# =====================================================
# CONFIG
# =====================================================

BUCKET_NAME = "enterprise-data-lake"

S3_ENDPOINT = os.getenv(
    "S3_ENDPOINT",
    "http://localhost:9000"
)
S3_ACCESS_KEY = "edladmin"
S3_SECRET_KEY = "edladmin123"


# Make Spark workers use the current virtual environment
os.environ["PYSPARK_PYTHON"] = sys.executable


# =====================================================
# S3 / RUSTFS CLIENT
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
    .appName("BronzeToSilver")
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
# FIND LATEST BRONZE OBJECT
# =====================================================

def get_latest_object(prefix):

    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=prefix
    )

    objects = [
        obj
        for obj in response.get("Contents", [])
        if not obj["Key"].endswith("/")
    ]

    if not objects:
        raise RuntimeError(
            f"No Bronze files found for prefix: {prefix}"
        )

    latest = max(
        objects,
        key=lambda x: x["LastModified"]
    )

    return latest["Key"]


# =====================================================
# DOWNLOAD OBJECT
# =====================================================

def download_object(key, local_path):

    print(f"Downloading: {key}")

    s3.download_file(
        BUCKET_NAME,
        key,
        str(local_path)
    )


# =====================================================
# UPLOAD SPARK PARQUET OUTPUT
# =====================================================
def upload_silver_dataset(
    dataframe,
    dataset_name,
    temp_directory
):

    print(f"Preparing {dataset_name} for Silver...")

    # Spark DataFrame -> Pandas
    pandas_df = dataframe.toPandas()

    # Write Parquet locally using PyArrow
    output_file = (
        Path(temp_directory)
        / f"{dataset_name}_{run_timestamp}.parquet"
    )
    
    pandas_df.to_parquet(
    output_file,
    engine="pyarrow",
    index=False,
    coerce_timestamps="us",
    allow_truncated_timestamps=True
)

    # Destination inside RustFS
    object_key = (
        f"silver/{dataset_name}/"
        f"processing_date={processing_date}/"
        f"{dataset_name}_{run_timestamp}.parquet"
    )

    # Upload Parquet to RustFS
    s3.upload_file(
        str(output_file),
        BUCKET_NAME,
        object_key
    )

    print(
        f"Uploaded -> "
        f"s3://{BUCKET_NAME}/{object_key}"
    )
    
# =====================================================
# MAIN
# =====================================================

with tempfile.TemporaryDirectory() as temp_dir:

    temp = Path(temp_dir)

    print()
    print("========================================")
    print("LOCATING LATEST BRONZE FILES")
    print("========================================")

    customers_key = get_latest_object(
        "bronze/postgres/customers/"
    )

    orders_key = get_latest_object(
        "bronze/postgres/orders/"
    )

    order_items_key = get_latest_object(
        "bronze/postgres/order_items/"
    )

    products_key = get_latest_object(
        "bronze/api/products/"
    )


    # =================================================
    # DOWNLOAD BRONZE
    # =================================================

    customers_file = temp / "customers.parquet"
    orders_file = temp / "orders.parquet"
    order_items_file = temp / "order_items.parquet"
    products_file = temp / "products.json"

    download_object(
        customers_key,
        customers_file
    )

    download_object(
        orders_key,
        orders_file
    )

    download_object(
        order_items_key,
        order_items_file
    )

    download_object(
        products_key,
        products_file
    )


    # =================================================
    # READ BRONZE WITH SPARK
    # =================================================

    print()
    print("========================================")
    print("READING BRONZE DATA")
    print("========================================")

    customers_raw = spark.read.parquet(
        str(customers_file)
    )

    orders_raw = spark.read.parquet(
        str(orders_file)
    )

    order_items_raw = spark.read.parquet(
        str(order_items_file)
    )

    products_payload = (
        spark.read
        .option("multiLine", "true")
        .json(str(products_file))
    )

    products_raw = (
        products_payload
        .select(
            explode("products").alias("product")
        )
    )


    print(
        f"Customers raw   : "
        f"{customers_raw.count():,}"
    )

    print(
        f"Orders raw      : "
        f"{orders_raw.count():,}"
    )

    print(
        f"Order Items raw : "
        f"{order_items_raw.count():,}"
    )

    print(
        f"Products raw    : "
        f"{products_raw.count():,}"
    )


    # =================================================
    # SILVER - CUSTOMERS
    # =================================================

    customers = (
        customers_raw
        .select(
            col("customer_id")
                .cast("long"),

            trim(
                col("first_name")
            ).alias("first_name"),

            trim(
                col("last_name")
            ).alias("last_name"),

            lower(
                trim(col("email"))
            ).alias("email"),

            trim(
                col("country")
            ).alias("country"),

            col("created_at")
                .cast("timestamp"),

            col("_ingested_at")
                .cast("timestamp")
        )
        .filter(
            col("customer_id").isNotNull()
        )
        .filter(
            col("email").isNotNull()
        )
        .dropDuplicates(
            ["customer_id"]
        )
        .withColumn(
            "_processed_at",
            current_timestamp()
        )
    )


    # =================================================
    # SILVER - PRODUCTS
    # =================================================

    products = (
        products_raw
        .select(
            col("product.id")
                .cast("long")
                .alias("product_id"),

            trim(
                col("product.title")
            ).alias("product_name"),

            lower(
                trim(col("product.category"))
            ).alias("category"),

            col("product.price")
                .cast("decimal(12,2)")
                .alias("unit_price"),

            col("product.stock")
                .cast("integer")
                .alias("stock"),

            col("product.rating")
                .cast("double")
                .alias("rating")
        )
        .filter(
            col("product_id").isNotNull()
        )
        .filter(
            col("unit_price") >= 0
        )
        .dropDuplicates(
            ["product_id"]
        )
        .withColumn(
            "_processed_at",
            current_timestamp()
        )
    )


    # =================================================
    # SILVER - ORDERS
    # =================================================

    valid_statuses = [
        "PAID",
        "SHIPPED",
        "DELIVERED",
        "CANCELLED"
    ]

    orders = (
        orders_raw
        .select(
            col("order_id")
                .cast("long"),

            col("customer_id")
                .cast("long"),

            col("order_timestamp")
                .cast("timestamp"),

            upper(
                trim(col("status"))
            ).alias("status"),

            col("total_amount")
                .cast("decimal(12,2)"),

            col("_ingested_at")
                .cast("timestamp")
        )
        .filter(
            col("order_id").isNotNull()
        )
        .filter(
            col("customer_id").isNotNull()
        )
        .filter(
            col("order_timestamp").isNotNull()
        )
        .filter(
            col("total_amount") >= 0
        )
        .filter(
            col("status").isin(valid_statuses)
        )
        .dropDuplicates(
            ["order_id"]
        )
    )


    # Referential integrity:
    # order must belong to a valid customer
    orders = (
        orders
        .join(
            customers.select(
                "customer_id"
            ),
            on="customer_id",
            how="left_semi"
        )
        .withColumn(
            "_processed_at",
            current_timestamp()
        )
    )


    # =================================================
    # SILVER - ORDER ITEMS
    # =================================================

    order_items = (
        order_items_raw
        .select(
            col("order_item_id")
                .cast("long"),

            col("order_id")
                .cast("long"),

            col("product_id")
                .cast("long"),

            col("quantity")
                .cast("integer"),

            col("unit_price")
                .cast("decimal(12,2)"),

            col("_ingested_at")
                .cast("timestamp")
        )
        .filter(
            col("order_item_id").isNotNull()
        )
        .filter(
            col("order_id").isNotNull()
        )
        .filter(
            col("product_id").isNotNull()
        )
        .filter(
            col("quantity") > 0
        )
        .filter(
            col("unit_price") >= 0
        )
        .dropDuplicates(
            ["order_item_id"]
        )
    )


    # Order must exist
    order_items = order_items.join(
        orders.select("order_id"),
        on="order_id",
        how="left_semi"
    )


    # Product must exist
    order_items = (
        order_items
        .join(
            products.select(
                "product_id"
            ),
            on="product_id",
            how="left_semi"
        )
        .withColumn(
            "_processed_at",
            current_timestamp()
        )
    )


    # =================================================
    # DATA QUALITY RESULTS
    # =================================================

    print()
    print("========================================")
    print("DATA QUALITY RESULTS")
    print("========================================")

    print(
        f"Customers : "
        f"{customers_raw.count():,}"
        f" -> {customers.count():,}"
    )

    print(
        f"Orders    : "
        f"{orders_raw.count():,}"
        f" -> {orders.count():,}"
    )

    print(
        f"Items     : "
        f"{order_items_raw.count():,}"
        f" -> {order_items.count():,}"
    )

    print(
        f"Products  : "
        f"{products_raw.count():,}"
        f" -> {products.count():,}"
    )


    # =================================================
    # WRITE SILVER
    # =================================================

    print()
    print("========================================")
    print("WRITING SILVER DATA")
    print("========================================")

    upload_silver_dataset(
        customers,
        "customers",
        temp_dir
    )

    upload_silver_dataset(
        products,
        "products",
        temp_dir
    )

    upload_silver_dataset(
        orders,
        "orders",
        temp_dir
    )

    upload_silver_dataset(
        order_items,
        "order_items",
        temp_dir
    )


# =====================================================
# FINISH
# =====================================================

spark.stop()

print()
print("========================================")
print("BRONZE -> SILVER COMPLETE")
print("========================================")