import pandas as pd
import json
import logging
from dotenv import load_dotenv
import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

load_dotenv()

def use_log():
    try:
            logging.basicConfig(
                filename="etl.log",
                level=logging.INFO,
                format="%(asctime)s - %(levelname)s - %(message)s"
            )
            
            print("LOG FILE CREATED")
    except FileNotFoundError as e:
            print("LOG FILE PATH ERROR")

ACCESS_KEY=os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY=os.getenv("AWS_SECRET_ACCESS_KEY")
REGION=os.getenv("AWS_DEFAULT_REGION")

s3=boto3.client("s3")


def upload_file_to_s3(local_path, bucket, s3_key):
    try:
        s3.upload_file(local_path, bucket, s3_key)
        logging.info(f"Uploaded {local_path} to {bucket}/{s3_key}")
    except FileNotFoundError:
        logging.error(f"Local file not found: {local_path}")
    except NoCredentialsError:
        logging.error("AWS credentials not found")
    except ClientError as e:
        logging.error(f"S3 error uploading {local_path}: {e}")
    


def download_file_from_s3(bucket, s3_key , file_name):
    try:
        s3.download_file(bucket, s3_key , file_name)
        logging.info(f"downloaded file from {bucket}/{s3_key}")
    except FileNotFoundError:
        logging.error(f"Local file not found in {bucket}/{s3_key} ")
    except NoCredentialsError:
        logging.error("AWS credentials not found")
    except ClientError as e:
        logging.error(f"S3 error downloading from {bucket}/{s3_key} : {e}")
    
    