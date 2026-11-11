from cloud_connect import upload_file_to_s3
from pathlib import Path
DATA_DIR = Path(__file__).parent / "data"
import boto3

bucket_name="da-project-2026"
s3=boto3.client("s3")

files = [
    "olist_customers_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
]
for f in files:
    upload_file_to_s3(DATA_DIR / f, bucket_name, f"raw/{f}")