from cloud_connect import download_file_from_s3
from cloud_connect import use_log
from pathlib import Path
import boto3
DATA_DIR = Path(__file__).parent / "downloads"

s3=boto3.client("s3")
bucket_name="da-project-2026"


"olist_customers_dataset_downloaded.csv",
"olist_order_items_dataset_downloaded.csv",
"olist_orders_dataset_downloaded.csv",
"olist_products_dataset_downloaded.csv",


download_file_from_s3(bucket_name, "raw/olist_customers_dataset.csv",DATA_DIR/"olist_customers_dataset_downloaded.csv")
download_file_from_s3(bucket_name, "raw/olist_order_items_dataset.csv",DATA_DIR/"olist_order_items_dataset_downloaded.csv")
download_file_from_s3(bucket_name, "raw/olist_orders_dataset.csv",DATA_DIR/"olist_orders_dataset_downloaded.csv")
download_file_from_s3(bucket_name, "raw/olist_products_dataset.csv",DATA_DIR/"olist_products_dataset_downloaded.csv")