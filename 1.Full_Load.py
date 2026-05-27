# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp


# COMMAND ----------

# MAGIC %run "/Workspace/Users/ivychemtai99@gmail.com/Consolidated_Pipeline/Utilities"

# COMMAND ----------

#  Widget Definitions 
dbutils.widgets.text("catalog",     "fmcg",   "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

#  Retrieve Widget Values 
catalog     = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

#  Path Configuration
base_path      = f"s3://sportsbar-childdata/orders/"
landing_path   = f"s3://sportsbar-childdata/orders/landing/"
processed_path = f"{base_path}/processed/"

print("Base Path:      ", base_path)
print("Landing Path:   ", landing_path)
print("Processed Path: ", processed_path)


# COMMAND ----------

#  Read CSV Files from Landing Path ────────────────────────────────────────
df = (
    spark.read
    .options(header=True, inferSchema=True)
    .csv(f"s3://sportsbar-childdata/orders/landing/*.csv")
    .withColumn("_ingest_timestamp", current_timestamp())
    .select("*", "_metadata.file_name", "_metadata.file_size")
)


df.display()


# COMMAND ----------

 
catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

bronze_table = f"{catalog}.bronze.{data_source}"

# Create Bronze Table  
(
    df.write
    .format("delta")
    .option("delta.enableChangeDataFeed", "true")
    .mode("append")
    .saveAsTable(bronze_table)
)

print(f"✅ Bronze table created: {bronze_table}")

# COMMAND ----------

files=dbutils.fs.ls(landing_path)
for file_info in files:
    dbutils.fs.mv(
        file_info.path,
        f"{processed_path}/{file_info.name}"
    )


# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

bronze_table = f"{catalog}.bronze.{data_source}"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver

# COMMAND ----------

# Read from bronze
df_orders = spark.sql(f"SELECT * FROM {bronze_table}")

# Assign to silver
df_silver = df_orders

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Cleaning

# COMMAND ----------

df_silver = (
    df_orders
    # 1. Keep only rows where order_qty is present
    .filter(F.col("order_qty").isNotNull())

    # 2. Clean customer_id → keep if numeric, else replace with '999999'
    .withColumn(
        "customer_id",
        F.when(
            F.col("customer_id").rlike(r"^[0-9]+$"),
            F.col("customer_id")
        )
        .otherwise("999999")
        .cast("string")
    )

  # Strip weekday name first
# "Monday, July 07, 2025" → "July 07, 2025"
.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)

# Step 2 - Then parse the cleaned date
.withColumn(
    "order_placement_date",
    F.coalesce(
        F.expr("try_to_date(order_placement_date, 'yyyy/MM/dd')"),
        F.expr("try_to_date(order_placement_date, 'dd/MM/yyyy')"),
        F.expr("try_to_date(order_placement_date, 'dd-MM-yyyy')"),
        F.expr("try_to_date(order_placement_date, 'MMMM dd, yyyy')"),
        F.lit("1900-01-01").cast("date")
    )
)

    # 5. Drop duplicates
    .dropDuplicates([
        "order_id",
        "order_placement_date",
        "customer_id",
        "product_id"
    ])
)
df_silver = df_silver.withColumn(
    "product_id",
    F.col("product_id").cast("string")
)

df_silver.display()






# COMMAND ----------

df_silver.selectExpr(
    "min(order_placement_date) as min_date",
    "max(order_placement_date) as max_date"
).show()

# COMMAND ----------

df_products = spark.table( "fmcg.silver.products")

# fmcg.silver.products
df_joined = df_silver.join(
    df_products.select("product_id", "product_code"),
    on="product_id",
    how="inner"
).select(
    df_silver["*"],
    df_products["product_code"]
)

df_joined.display()







# COMMAND ----------

silver_table = f"{catalog}.silver.{data_source}"
if not (spark.catalog.tableExists(silver_table)):
    # Create silver table if it doesn't exist
    df_joined.write \
        .format("delta") \
        .option("delta.enableChangeDataFeed", "true") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .saveAsTable(silver_table)
else:
    # Upsert if table already exists
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias("silver").merge(
        df_joined.alias("bronze"),
        """
        silver.order_placement_date = bronze.order_placement_date AND
        silver.order_id = bronze.order_id AND
        silver.product_code = bronze.product_code AND
        silver.customer_id = bronze.customer_id
        """
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

print(f"✅ Silver table written: {silver_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Gold

# COMMAND ----------

df_gold = spark.sql(f"""
    SELECT
        order_id,
        order_placement_date AS date,
        customer_id AS customer,
        product_id,
        product_code,
        order_qty AS sold_quantity
    FROM {silver_table}
""")

df_gold.show(2)

# COMMAND ----------

gold_table = f"{catalog}.gold.sb_fact_orders"

if not (spark.catalog.tableExists(gold_table)):
    print("Creating New Table")
    df_gold.write \
        .format("delta") \
        .option("delta.enableChangeDataFeed", "true") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("source").merge(
        df_gold.alias("gold"),
        """
        source.date = gold.date AND
        source.order_id = gold.order_id AND
        source.product_code = gold.product_code AND
        source.customer_id = gold.customer_id
        """
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

print(f"✅ Gold table written: {gold_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Merge with Parent company

# COMMAND ----------

df_child = spark.sql(f"""
    SELECT
        date,
        product_code,
        customer,
        sold_quantity
    FROM {gold_table}
""")

df_child.show(10)

# COMMAND ----------

# First change the date to first day of the month
# 2025-07-12 --> 2025-07-01
# 2025-07-13 --> 2025-07-01

df_monthly = (
    df_child
    # 1. Get month start date (e.g., 2025-11-30 → 2025-11-01)
    .withColumn("month_start", F.trunc("date", "MM"))

    # 2. Group at monthly grain by month_start + product_code + customer
    .groupBy("month_start", "product_code", "customer")
    .agg(
        F.sum("sold_quantity").alias("sold_quantity")
        
    )
    # 3. Rename month_start to date
    .withColumnRenamed("month_start", "date")
)

df_monthly.show(10)

# COMMAND ----------

# Rename month_start to date and customer to customer_code
df_monthly = df_monthly \
    .withColumnRenamed("month_start", "date") \
    .withColumnRenamed("customer", "customer_code")

# Merge with parent gold table
gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")

gold_parent_delta.alias("parent_gold").merge(
    df_monthly.alias("child_gold"),
    """
    parent_gold.date = child_gold.date AND
    parent_gold.product_code = child_gold.product_code AND
    parent_gold.customer_code = child_gold.customer_code
    """
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

print("✅ Merge with parent completed successfully!")