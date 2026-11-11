#%%
import os
import sys
from functools import reduce
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import SparkSession

import os
import sys




BASE_DIR = Path(__file__).resolve().parent
# ------------------------------------------



os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("olist_data") \
    .getOrCreate()
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder \
    .appName("olist_data") \
    .getOrCreate()

customers_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("downloads/olist_customers_dataset_downloaded.csv")
print("data frame customers_df created")

order_items_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("downloads/olist_order_items_dataset_downloaded.csv")
print("data frame order_items_df created")

orders_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("downloads/olist_orders_dataset_downloaded.csv")
print("data frame orders_df created")

products_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("downloads/olist_products_dataset_downloaded.csv")
print("data frame products_df created")

#%%
# TO CHECK THE NULL VALUES IN EACH DATAFRAME

dataframes = [
    ("customers_df", customers_df),
    ("order_items_df", order_items_df),
    ("orders_df", orders_df),
    ("products_df", products_df)
]

for name, df in dataframes:
    print("_________________________________________")
    print(f"Dataset: {name}")

    total_rows = df.count()
    distinct_rows = df.distinct().count()
    print(f"NUMBER OF DUPLICATE RECORDS: {total_rows - distinct_rows}")

    any_null_condition = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in df.columns])

    null_row_count = df.filter(any_null_condition).count()
    print(f"NUMBER OF ROWS WITH AT LEAST ONE NULL: {null_row_count}")

#%%


# CHECKS NULL VALUES IN ORDERS_DF
any_null_condition = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in orders_df.columns])
orders_df.filter(any_null_condition).show()

#%%


# REMOVE CANCELED ORDERS (null-safe: rows with null status are kept)
orders_df = orders_df.filter(
    F.col("order_status").isNull() | (F.col("order_status") != "canceled")
)

#%%


# TO FIND THE NULL VALUES WITH ORDER STATUS AS DELIVERED
orders_df.filter(
    (orders_df["order_status"] == "delivered") &
    ((orders_df["order_delivered_carrier_date"].isNull()) | (orders_df["order_delivered_customer_date"].isNull()))
).show()

#%%
# TO CHANGE THE NULL VALUES AS UNAVAILABLE

# 1. Reuse or define your bad condition
bad_delivered_condition = (
    (F.col("order_status") == "delivered") &
    (
        F.col("order_delivered_carrier_date").isNull() | 
        F.col("order_delivered_customer_date").isNull()
    )
)

# 2. Filter the DataFrame to keep ONLY the rows that DO NOT match the bad condition
orders_df = orders_df.filter(~bad_delivered_condition)

#%%

#TO CHECK NAY NULL VALUES IN order_delivered_carrier_date WHERE ORDER STATUS IS SHIPPED
orders_df.filter(
    (orders_df["order_status"] == "shipped") &
    orders_df["order_delivered_carrier_date"].isNull()
).show()

#%%


# CHECK NULL VALUES IN ORDERS_DF AFTER THE UPDATE
any_null_condition = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in products_df.columns])
products_df.filter(any_null_condition).show()


# %%

# FILL NULL VALUES IN product_category_name WITH UNKNOWN
products_df = products_df.withColumn(
    "product_category_name",
    F.coalesce(F.col("product_category_name"), F.lit("unknown"))
)


# %%

# FILL NULL IN "product_name_lenght" with median
# 1. Calculate the median of the column (PySpark ignores nulls automatically)
median_length = products_df.approxQuantile("product_name_lenght", [0.5], 0.0)[0]

# 2. Fill the null values with that median
products_df = products_df.na.fill({"product_name_lenght": median_length})
# %%
 
# FILL NULL IN product_description_lenght WITH MEDIAN
median_length = products_df.approxQuantile("product_description_lenght", [0.5], 0.0)[0]

products_df = products_df.na.fill({"product_description_lenght": median_length})

# %%

#FILL NULL IN product_photos_qty WITH MEDIAN

median_length = products_df.approxQuantile("product_photos_qty", [0.5], 0.0)[0]

products_df = products_df.na.fill({"product_photos_qty": median_length})


# %%

#FILL NULL IN product_weight_g WITH MEDIAN

median_length = products_df.approxQuantile("product_weight_g", [0.5], 0.0)[0]

products_df = products_df.na.fill({"product_weight_g": median_length})

# %%

#FILL NULL IN product_length_cm WITH MEDIAN

median_length = products_df.approxQuantile("product_length_cm", [0.5], 0.0)[0]

products_df = products_df.na.fill({"product_length_cm": median_length})


# %%

#FILL NULL IN product_height_cm WITH MEDIAN

median_length = products_df.approxQuantile("product_height_cm", [0.5], 0.0)[0]

products_df = products_df.na.fill({"product_height_cm": median_length})


# %%


#FILL NULL IN product_width_cm WITH MEDIAN

median_length = products_df.approxQuantile("product_width_cm", [0.5], 0.0)[0]

products_df = products_df.na.fill({"product_width_cm": median_length})


# %%
dataframes = [
    ("customers_df", customers_df),
    ("order_items_df", order_items_df),
    ("orders_df", orders_df),
    ("products_df", products_df)
]

for name,df in dataframes:
    print(f"Dataset:{name}")
    df.printSchema()
    

# %%

# CONVERTING PYSPARK DF TO PANDAS TO BYPASS HADOOP

customers_df.toPandas().to_csv(BASE_DIR / "processed" / "customers_cleaned.csv", index=False,mode='w')
order_items_df.toPandas().to_csv(BASE_DIR / "processed" / "order_items_cleaned.csv", index=False,mode='w')
orders_df.toPandas().to_csv(BASE_DIR / "processed" / "orders_cleaned.csv", index=False,mode='w')
products_df.toPandas().to_csv(BASE_DIR / "processed" / "products_cleaned.csv", index=False,mode='w')


# 1. Join order_items with orders
order_complete_df = order_items_df.join(
    orders_df, 
    on="order_id", 
    how="inner"
)

# 2. Join with customers (using customer_id from orders)
order_complete_df = order_complete_df.join(
    customers_df, 
    on="customer_id", 
    how="inner"
)

# 3. Join with products (using product_id)
master_df = order_complete_df.join(
    products_df, 
    on="product_id", 
    how="left"  # Left join protects against missing product entries
)

# Preview the combined schema
master_df.printSchema()

master_df = master_df.withColumn(
    "total_value", 
    F.col("price") + F.col("freight_value")
)
master_df.toPandas().to_csv(BASE_DIR / "processed" / "full_joined_dataset.csv", index=False,mode='w')



