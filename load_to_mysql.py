from cloud_connect import use_log
from pathlib import Path
import pandas as pd
import logging
from dotenv import load_dotenv
import os
import mysql.connector
use_log()
load_dotenv()

DATA_DIR=Path(__file__).resolve().parent

daily_orders_df=pd.read_csv(DATA_DIR/"processed/aggregations/daily_orders.csv")
monthly_rev_by_state_df=pd.read_csv(DATA_DIR/"processed/aggregations/monthly_rev_by_state.csv")
top_20_customers_df=pd.read_csv(DATA_DIR/"processed/aggregations/top_20_customers.csv")
top_20_products_df=pd.read_csv(DATA_DIR/"processed/aggregations/top_20_products.csv")



load_dotenv()
DB_HOST=os.getenv("DB_HOST")
DB_USER=os.getenv("DB_USER")
DB_PASSWORD=os.getenv("DB_PASSWORD")
DB_NAME=os.getenv("DB_NAME")

try:
    connection=mysql.connector.connect(

        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    print("DATABASE CONNECTION SUCESSFULL")
    logging.info("Connected to Database")
except Exception as e :
    print("DATABASE CONNECTION UNSUCESSFULL")
    logging.error(f"Database Connection Error {e}")

cursor = connection.cursor()

query="""
INSERT INTO daily_orders(order_date,total_orders,total_revenue)
VALUES (%s,%s,%s)
"""
data = list(daily_orders_df.itertuples(index=False, name=None))
cursor.executemany(query,data)
print(cursor.rowcount, "records inserted successfully!")


query="""
INSERT INTO monthly_rev_by_state(year,month,month_name,customer_state,total_orders,total_revenue)
VALUES (%s,%s,%s,%s,%s,%s)
"""
data = list(monthly_rev_by_state_df.itertuples(index=False, name=None))
cursor.executemany(query,data)
print(cursor.rowcount, "records inserted successfully!")

query="""
INSERT INTO top_20_customers(customer_unique_id,total_orders,total_spend)
VALUES (%s,%s,%s)
"""
data = list(top_20_customers_df.itertuples(index=False, name=None))
cursor.executemany(query,data)
print(cursor.rowcount, "records inserted successfully!")

query="""
INSERT INTO top_20_products(product_category_name,total_orders,total_revenue)
VALUES (%s,%s,%s)
"""
data = list(top_20_products_df.itertuples(index=False, name=None))
cursor.executemany(query,data)
print(cursor.rowcount, "records inserted successfully!")


connection.commit()
