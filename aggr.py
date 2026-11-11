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


df=pd.read_csv(DATA_DIR/"processed/full_joined_dataset.csv")
df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"])
df["order_date"]=df["order_purchase_timestamp"].dt.date
daily_orders=df.groupby("order_date").agg(
    total_orders=("order_id","nunique"),
    total_revenue=("total_value","sum")

).sort_index()
print(daily_orders)
daily_orders.to_csv(DATA_DIR/"processed/aggregations/daily_orders.csv",mode="w")

top_products=df.groupby("product_category_name").agg(
    total_orders=("order_id", "nunique"),
    total_revenue=("total_value","sum")
).sort_index().sort_values(by="total_revenue",ascending=False).head(20)
print(top_products)

top_products.to_csv(DATA_DIR/"processed/aggregations/top_20_products.csv",mode="w")

top_customers=df.groupby("customer_unique_id").agg(
     total_orders=("order_id", "nunique"),
     total_spend=("total_value","sum")
).sort_index().sort_values(by="total_spend",ascending=False).head(20)
print(top_products)

top_customers.to_csv(DATA_DIR/"processed/aggregations/top_20_customers.csv",mode="w")


df["year"] = df["order_purchase_timestamp"].dt.year
df["month"] = df["order_purchase_timestamp"].dt.month
df["month_name"]=df["order_purchase_timestamp"].dt.month_name()


monthly_rev_by_state=df.groupby(["year","month","month_name","customer_state"]).agg(
     total_orders=("order_id", "nunique"),
    total_revenue=("total_value","sum")
).reset_index().sort_values(by=["year","month", "total_revenue"], ascending=[True,True, False])
print(monthly_rev_by_state)
monthly_rev_by_state.to_csv(DATA_DIR/"processed/aggregations/monthly_rev_by_state.csv",mode="w",index=False)

#configure s3
bucket_name="da-project-2026"
s3=boto3.client("s3")

#upload to s3
local_file_path=DATA_DIR/"processed"/"aggregations"

files=[
    "daily_orders.csv",
    "monthly_rev_by_state.csv",
    "top_20_customers.csv",
    "top_20_products.csv"
]

for f in files:
    upload_file_to_s3(local_file_path/f,bucket_name,f"curated/{f}")

