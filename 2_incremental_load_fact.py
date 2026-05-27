# Databricks notebook source
# MAGIC %md
# MAGIC **Import Required Libraries**

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC **Load Project Utilities & Initialize Notebook Widgets**

# COMMAND ----------

# MAGIC %run "/Workspace/Users/ivychemtai99@gmail.com/Consolidated_Pipeline/Utilities"

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-childdata/{data_source}'
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"
print("Base Path: ", base_path)
print("Landing Path: ", landing_path)
print("Processed Path: ", processed_path)


# define the tables
bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze

# COMMAND ----------

df = spark.read.options(header=True, inferSchema=True).csv(f"{landing_path}/*.csv").withColumn("read_timestamp", F.current_timestamp()).select("*", "_metadata.file_name", "_metadata.file_size")



# COMMAND ----------

# Read from correct bronze table
df_bronze = spark.table(f"{catalog}.{bronze_schema}.{data_source}")

# Cast order_placement_date to string to avoid schema conflict
df_bronze = df_bronze.withColumn(
    "order_placement_date",
    F.col("order_placement_date").cast("string")
)

# Write to silver staging
df_bronze.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{silver_schema}.staging_{data_source}")

print(f"✅ Staging table recreated: {catalog}.{silver_schema}.staging_{data_source}")

# Verify
spark.table(f"{catalog}.{silver_schema}.staging_{data_source}").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Moving files from source to processed directory

# COMMAND ----------

files = dbutils.fs.ls(landing_path)
for file_info in files:
    dbutils.fs.mv(
        file_info.path,
        f"{processed_path}/{file_info.name}",
        True
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver

# COMMAND ----------

# Replace this
df_orders = spark.table(f"{catalog}.bronze.orders")

# With this
df_orders = spark.table(f"{catalog}.silver.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformations**

# COMMAND ----------

# 1. Keep only rows where order_qty is present
df_orders = df_orders.filter(F.col("order_qty").isNotNull())


# 2. Clean customer_id → keep numeric, else set to 999999
df_orders = df_orders.withColumn(
    "customer_id",
    F.when(F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id"))
     .otherwise("999999")
     .cast("string")
)

# 3. Remove weekday name from the date text
#    "Tuesday, July 01, 2025" → "July 01, 2025"
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)

# 4. Parse order_placement_date using multiple possible formats
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date("order_placement_date", "yyyy/MM/dd"),
        F.try_to_date("order_placement_date", "dd-MM-yyyy"),
        F.try_to_date("order_placement_date", "dd/MM/yyyy"),
        F.try_to_date("order_placement_date", "MMMM dd, yyyy"),
    )
)

# 5. Drop duplicates
df_orders = df_orders.dropDuplicates(["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"])

# 5. convert product id to string
df_orders = df_orders.withColumn('product_id', F.col('product_id').cast('string'))

# COMMAND ----------

# check what's the maximum and minimum date
df_orders.agg(
    F.min("order_placement_date").alias("min_date"),
    F.max("order_placement_date").alias("max_date")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Join with products**

# COMMAND ----------

df_products = spark.table("fmcg.silver.products")
df_joined = df_orders.join(df_products, on="product_id", how="inner").select(df_orders["*"], df_products["product_code"])

df_joined.show(5)

# COMMAND ----------

# Load both DataFrames
df_silver = spark.table("fmcg.silver.staging_orders")
df_products = spark.table("fmcg.silver.products")  # adjust table name as needed
df_joined = df_silver.alias("orders").join(
    df_products.alias("products"),
    on="product_id",
    how="inner"
).select(
    F.col("orders.order_id"),
    F.col("orders.order_placement_date"),
    F.col("orders.customer_id"),
    F.col("orders.product_id"),
    F.col("orders.order_qty"),
    F.col("orders._ingest_timestamp"),
    F.col("orders.file_name"),
    F.col("orders.file_size"),
    F.col("products.product_code")  # explicitly from products table
)

df_joined.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Staging table to process just the arrived incremenal data

# COMMAND ----------

# stagging for incremental data

df_joined.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("overwriteSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.staging_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {catalog}.{silver_schema}.staging_{data_source};")

df_gold.show(2)

# COMMAND ----------

df_gold.count()

# COMMAND ----------

# DBTITLE 1,Cell 27
from pyspark.sql.functions import try_to_date, coalesce

# Convert date column from STRING to DATE type - handles both formats
df_gold = df_gold.withColumn(
    "date", 
    coalesce(
        try_to_date("date", "yyyy/MM/dd"),  # Try YYYY/MM/DD first
        try_to_date("date", "dd-MM-yyyy")   # Fall back to DD-MM-YYYY
    )
)

# Deduplicate
df_gold = df_gold.dropDuplicates([
    "date",
    "product_code",
    "customer_code"
])

# Merge
gold_delta = DeltaTable.forName(spark, gold_table)
gold_delta.alias("gold").merge(
    df_gold.alias("source"),
    """
    gold.date = source.date AND
    gold.product_code = source.product_code AND
    gold.customer = source.customer_code
    """
).whenMatchedUpdate(set={
    "sold_quantity": "source.sold_quantity"
}).whenNotMatchedInsert(values={
    "order_id"     : "source.order_id",
    "date"         : "source.date",
    "product_code" : "source.product_code",
    "product_id"   : "source.product_id",
    "customer"     : "source.customer_code",
    "sold_quantity": "source.sold_quantity"
}).execute()

print(f"✅ Gold table written: {gold_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merging with Parent company

# COMMAND ----------

# MAGIC %md
# MAGIC - Note: We want data for monthly level but child data is on daily level

# COMMAND ----------

# MAGIC %md
# MAGIC **Incremental Load**

# COMMAND ----------

from pyspark.sql.functions import try_to_date, coalesce

# df_child = your incremental daily rows
df_child = spark.sql(f"SELECT order_placement_date as date FROM {catalog}.{silver_schema}.staging_{data_source}")

# Convert STRING to DATE type - handles both YYYY/MM/DD and DD-MM-YYYY formats
df_child = df_child.withColumn(
    "date",
    coalesce(
        try_to_date("date", "yyyy/MM/dd"),  # Try YYYY/MM/DD first
        try_to_date("date", "dd-MM-yyyy")   # Fall back to DD-MM-YYYY
    )
)

incremental_month_df = df_child.select(
    F.trunc("date", "MM").alias("start_month")
).distinct()

incremental_month_df.show()

incremental_month_df.createOrReplaceTempView("incremental_months")

# COMMAND ----------

# DBTITLE 1,Cell 32
monthly_table = spark.sql(f"""
    SELECT 
        date,
        product_code,
        customer as customer_code,
        sold_quantity
    FROM {catalog}.{gold_schema}.sb_fact_orders sbf
    INNER JOIN incremental_months m
        ON trunc(sbf.date, 'MM') = m.start_month
""")

print("Total Rows: ", monthly_table.count())
monthly_table.show(10)

# COMMAND ----------

monthly_table.select('date').distinct().orderBy('date').show()

# COMMAND ----------

df_monthly_recalc = (
    monthly_table
    .withColumn("month_start", F.trunc("date", "MM"))
    .groupBy("month_start", "product_code", "customer_code")
    .agg(F.sum("sold_quantity").alias("sold_quantity"))
    .withColumnRenamed("month_start", "date")   # month_start → date = first of month
)

df_monthly_recalc.show(10, truncate=False)

# COMMAND ----------

df_monthly_recalc.count()

# COMMAND ----------

gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
gold_parent_delta.alias("parent_gold").merge(df_monthly_recalc.alias("child_gold"), "parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

df_gold.printSchema()
print(df_gold.select("date").distinct().show(10, truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup