import pandas as pd
import pyarrow
import logging
import json
import logging
from dotenv import load_dotenv
import os
import boto3
from pathlib import Path
from botocore.exceptions import ClientError, NoCredentialsError
from cloud_connect import *
use_log()

DATA_DIR=Path(__file__).resolve().parent
RAW_DIR=DATA_DIR/"data"
FINAL_DIR=DATA_DIR/"processed"/"full_joined_dataset.csv"
raw_files = [
    "olist_customers_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_products_dataset.csv"
]

report_data = []

for filename in raw_files:
    file_path = RAW_DIR / filename
    dataset_name = filename
    if file_path.exists():
        df = pd.read_csv(file_path)
        num_rows=len(df)
        num_cols=len(df.columns)
        duplicate_rows = df.duplicated().sum()
        null_cells=df.isnull().sum().sum()
        total_cells = num_rows * num_cols
        null_percentage = (null_cells / total_cells * 100) if total_cells > 0 else 0.0
        report_data.append({
            "Dataset_Stage": "Raw",
            "Dataset_Name": filename,
            "Row_Count": num_rows,
            "Duplicate_Rows": duplicate_rows,
            "Null_Cells": null_cells,
            "Null_Percentage (%)": round(null_percentage, 4)}
        )
    else:
        print(f"Warning: Raw file not found -> {file_path}")
if FINAL_DIR.exists():
    df_final = pd.read_csv(FINAL_DIR)
    report_data.append({
        "Dataset_Stage": "Final Cleaned",
        "Dataset_Name": "full_joined_dataset.csv",
        "Row_Count": len(df_final),
        "Duplicate_Rows": df_final.duplicated().sum(),
        "Null_Cells": df_final.isnull().sum().sum(),
        "Null_Percentage (%)": round((df_final.isnull().sum().sum() / (len(df_final) * len(df_final.columns)) * 100), 4) if len(df_final) > 0 else 0.0
        })
else:
    print("Final cleaned file path not found.")

report_df = pd.DataFrame(report_data)
report_df.to_csv(DATA_DIR/"reports/report.csv",mode="w")
logging.info(f"Report file added to reports")

bucket_name="da-project-2026"
s3=boto3.client("s3")

upload_file_to_s3(DATA_DIR/"reports/report.csv",bucket_name,"reports/report.csv")

