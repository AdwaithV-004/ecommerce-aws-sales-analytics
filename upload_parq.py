from cloud_connect import upload_file_to_s3
from cloud_connect import use_log
from pathlib import Path
import pandas as pd
import pyarrow
import logging
import json
import logging
from dotenv import load_dotenv
import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
use_log()



DATA_DIR = Path(__file__).resolve().parent

master_df = pd.read_csv(DATA_DIR / "processed/full_joined_dataset.csv")
master_df["order_purchase_timestamp"] = pd.to_datetime(master_df["order_purchase_timestamp"])
master_df["year"] = master_df["order_purchase_timestamp"].dt.year
master_df["month"] = master_df["order_purchase_timestamp"].dt.month

try:
    master_df.to_parquet(
        DATA_DIR/"processed/parquet",
        engine="pyarrow",
        partition_cols=["year", "month"],
        index=False
    )
    logging.info("Parquest files saves to local directory")
    print("PARQUET FILES SAVED")
except Exception as e:
    logging.error(" Error saving Parquest files  to local directory")
    print(f"Error details: {e}")


DATA_DIR = Path(__file__).resolve().parent
local_parquet_dir = DATA_DIR / "processed" / "parquet"

# S3 Configuration
bucket_name="da-project-2026" 
s3_target_prefix = "processed/"  

s3_client = boto3.client('s3')

try:
    print("Starting upload of partitioned Parquet files to S3...")
    
    # Walk through local directory structure
    for root, dirs, files in os.walk(local_parquet_dir):
        for file in files:
            local_file_path = Path(root) / file
            
            #relative path
            relative_path = local_file_path.relative_to(local_parquet_dir)
            
            # S3 key
            s3_key = f"{s3_target_prefix}{str(relative_path).replace(os.sep, '/')}"
            
            print(f"Uploading: {relative_path} to s3://{bucket_name}/{s3_key}")
            
            
            upload_file_to_s3(str(local_file_path), bucket_name, s3_key)
            
    print("SUCCESS: All parquet files and partitions uploaded to S3!")
    logging.info("Parquet files successfully uploaded to S3.")

except Exception as e:
    print(f"ERROR: Failed to upload files to S3. Details: {e}")
    logging.error("Error uploading Parquet files to S3")


