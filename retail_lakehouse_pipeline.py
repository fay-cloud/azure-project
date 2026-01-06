
# =========================================
# Retail Lakehouse Pipeline (Bronze → Silver → Gold)
# =========================================
# This script assumes:
# - Data already exists in ADLS Gen2 under bronze/
# - Files are parquet for SQL-sourced tables
# - Customers table is CSV sourced from GitHub
# =========================================

from pyspark.sql.functions import col, sum, countDistinct, avg

# -----------------------------------------
# 1. Mount ADLS Gen2 (BEST PRACTICE)
# -----------------------------------------
# NOTE:
# - Replace <storage-account>, <container>, <scope>, <key>
# - Do NOT hardcode keys in production

# dbutils.fs.mount(
#     source="abfss://retail@<storage-account>.dfs.core.windows.net",
#     mount_point="/mnt/retail_project",
#     extra_configs={
#         "fs.azure.account.key.<storage-account>.dfs.core.windows.net":
#         dbutils.secrets.get(scope="<scope>", key="<key>")
#     }
# )

# -----------------------------------------
# 2. Inspect Bronze Layer
# -----------------------------------------
dbutils.fs.ls("/mnt/retail_project/bronze/")

# -----------------------------------------
# 3. Read Bronze Data
# -----------------------------------------

# Transactions = Orders + OrderItems already flattened upstream
df_transactions = spark.read.parquet(
    "/mnt/retail_project/bronze/transaction/"
)

df_products = spark.read.parquet(
    "/mnt/retail_project/bronze/product/"
)

df_stores = spark.read.parquet(
    "/mnt/retail_project/bronze/store/"
)

# Customers is CSV from GitHub
df_customers = spark.read.option("header", True).option(
    "inferSchema", True
).csv(
    "/mnt/retail_project/bronze/customer/"
)

# -----------------------------------------
# 4. Silver Layer (Cleaning + Typing)
# -----------------------------------------

df_transactions_clean = df_transactions.select(
    col("OrderID").cast("int").alias("transaction_id"),
    col("CustomerID").cast("int").alias("customer_id"),
    col("ProductID").cast("int").alias("product_id"),
    col("StoreID").cast("int").alias("store_id"),
    col("Quantity").cast("int").alias("quantity"),
    col("OrderDate").cast("date").alias("transaction_date")
)

df_products_clean = df_products.select(
    col("ProductID").cast("int").alias("product_id"),
    col("ProductName").alias("product_name"),
    col("Price").cast("double").alias("price")
)

df_stores_clean = df_stores.select(
    col("StoreID").cast("int").alias("store_id"),
    col("StoreName").alias("store_name"),
    col("City").alias("location"),
    col("Country")
)

df_customers_clean = df_customers.select(
    col("CustomerID").cast("int").alias("customer_id"),
    "FirstName",
    "LastName",
    "Email",
    "City",
    "CreatedDate"
).dropDuplicates(["customer_id"])

# -----------------------------------------
# 5. Build Silver Fact Table
# -----------------------------------------

df_silver = (
    df_transactions_clean
    .join(df_customers_clean, "customer_id", "left")
    .join(df_products_clean, "product_id", "left")
    .join(df_stores_clean, "store_id", "left")
    .withColumn("total_amount", col("quantity") * col("price"))
)

# -----------------------------------------
# 6. Write Silver Layer
# -----------------------------------------

silver_path = "/mnt/retail_project/silver/retail_sales_fact"

df_silver.write.mode("overwrite").format("delta").save(silver_path)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS retail_silver_sales
USING DELTA
LOCATION '{silver_path}'
""")

# -----------------------------------------
# 7. Gold Layer Aggregations
# -----------------------------------------

silver_df = spark.read.format("delta").load(silver_path)

gold_df = silver_df.groupBy(
    "transaction_date",
    "product_id", "product_name",
    "store_id", "store_name", "location", "Country"
).agg(
    sum("quantity").alias("total_quantity_sold"),
    sum("total_amount").alias("total_sales_amount"),
    countDistinct("transaction_id").alias("number_of_transactions"),
    avg("total_amount").alias("average_transaction_value")
)

# -----------------------------------------
# 8. Write Gold Layer
# -----------------------------------------

gold_path = "/mnt/retail_project/gold/retail_sales_summary"

gold_df.write.mode("overwrite").format("delta").save(gold_path)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS retail_gold_sales_summary
USING DELTA
LOCATION '{gold_path}'
""")

# -----------------------------------------
# END OF PIPELINE
# -----------------------------------------
