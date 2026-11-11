from cloud_connect import upload_file_to_s3
from pathlib import Path
DATA_DIR = Path(__file__).parent / "data"
import boto3

bucket_name="da-project-2026"
s3=boto3.client("s3")

upload_file_to_s3(DATA_DIR/"Final Project DE - E-Commerce Data Pipeline.pdf",bucket_name,"raw/Final Project DE - E-Commerce Data Pipeline.pdf")