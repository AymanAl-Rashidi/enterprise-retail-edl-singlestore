import io
import json
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
load_dotenv()

import boto3
import pandas as pd
import psycopg2
import requests

from botocore.config import Config


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

POSTGRES_CONFIG = {
    "host": os.getenv(
        "POSTGRES_HOST",
        "localhost"
    ),
    "port": 5432,
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

# =====================================================
# CONNECTIONS
# =====================================================

postgres_conn = psycopg2.connect(**POSTGRES_CONFIG)

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name="us-east-1",
    config=Config(
        signature_version="s3v4",
        s3={
            "addressing_style": "path"
        }
    )
)


# =====================================================
# INGESTION METADATA
# =====================================================

now = datetime.now(timezone.utc)

run_timestamp = now.strftime("%Y%m%d_%H%M%S")
ingestion_date = now.strftime("%Y-%m-%d")


# =====================================================
# FUNCTION:
# POSTGRES TABLE -> PARQUET -> BRONZE
# =====================================================

def ingest_postgres_table(table_name):

    print(f"Extracting {table_name} from PostgreSQL...")

    query = f"SELECT * FROM {table_name};"

    df = pd.read_sql_query(
        query,
        postgres_conn
    )

    df["_ingested_at"] = now

    print(
        f"{table_name}: {len(df):,} rows extracted"
    )

    buffer = io.BytesIO()

    df.to_parquet(
        buffer,
        engine="pyarrow",
        index=False
    )

    buffer.seek(0)

    object_key = (
        f"bronze/postgres/{table_name}/"
        f"ingestion_date={ingestion_date}/"
        f"{table_name}_{run_timestamp}.parquet"
    )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=object_key,
        Body=buffer.getvalue()
    )

    print(
        f"Uploaded -> s3://{BUCKET_NAME}/{object_key}"
    )


# =====================================================
# POSTGRESQL SOURCES
# =====================================================

tables = [
    "customers",
    "orders",
    "order_items"
]

for table in tables:
    ingest_postgres_table(table)


# =====================================================
# REST API -> RAW JSON -> BRONZE
# =====================================================

print()
print("Extracting products from DummyJSON API...")

response = requests.get(
    "https://dummyjson.com/products?limit=0",
    timeout=30
)

response.raise_for_status()

products_payload = response.json()

products_key = (
    f"bronze/api/products/"
    f"ingestion_date={ingestion_date}/"
    f"products_{run_timestamp}.json"
)

s3.put_object(
    Bucket=BUCKET_NAME,
    Key=products_key,
    Body=json.dumps(
        products_payload,
        ensure_ascii=False
    ).encode("utf-8"),
    ContentType="application/json"
)

print(
    f"Products: {len(products_payload['products']):,} rows"
)

print(
    f"Uploaded -> s3://{BUCKET_NAME}/{products_key}"
)


# =====================================================
# CLOSE
# =====================================================

postgres_conn.close()


print()
print("==============================================")
print("BRONZE INGESTION COMPLETE")
print("==============================================")