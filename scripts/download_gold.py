from pathlib import Path
import os
from dotenv import load_dotenv

import boto3
from botocore.config import Config


BUCKET_NAME = "enterprise-data-lake"

S3_ENDPOINT = os.getenv(
    "S3_ENDPOINT",
    "http://localhost:9000"
)


load_dotenv()

S3_ACCESS_KEY = os.getenv("RUSTFS_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("RUSTFS_SECRET_KEY")

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


datasets = [
    "daily_sales",
    "customer_360",
    "product_performance"
]


PROJECT_ROOT = Path(__file__).resolve().parents[1]
output_dir = PROJECT_ROOT / "data" / "gold_export"
output_dir.mkdir(parents=True, exist_ok=True)


for dataset in datasets:

    prefix = f"gold/{dataset}/"

    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=prefix
    )

    objects = [
        obj
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    latest = max(
        objects,
        key=lambda x: x["LastModified"]
    )

    output_file = output_dir / f"{dataset}.parquet"

    s3.download_file(
        BUCKET_NAME,
        latest["Key"],
        str(output_file)
    )

    print(
        f"Downloaded {dataset} -> {output_file}"
    )