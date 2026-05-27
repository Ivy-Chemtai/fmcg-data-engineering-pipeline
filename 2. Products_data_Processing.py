# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable
 

# COMMAND ----------

# MAGIC %md
# MAGIC ### loading project utilities and initializing widgets

# COMMAND ----------

# MAGIC %run /Workspace/Users/ivychemtai99@gmail.com/Consolidated_Pipeline/Utilities

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("schema", "bronze", "Schema")
dbutils.widgets.text("data_source", "products", "Data Source")
dbutils.widgets.text("file_path", "s3://your-bucket/products/products.csv", "File Path")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Bronze

# COMMAND ----------

base_path = "s3://sportsbar-childdata/products/"

# COMMAND ----------

df = (
    spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(base_path)
        .withColumn("read_timestamp", F.current_timestamp())
        .select(
            "*",
            F.col("_metadata.file_name").alias("file_name"),
            F.col("_metadata.file_size").alias("file_size")
        )
)

display(df)

# COMMAND ----------

catalog = "fmcg"
schema = "bronze"
data_source = "products"

# COMMAND ----------

df.write\
    .format("delta")\
    .option("deltaenabledChangeDataFeed", True)\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{schema}.{data_source}")


# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver

# COMMAND ----------

df_bronze = spark.read.table(f"{catalog}.bronze.{data_source}")
display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ###drop duplicates

# COMMAND ----------

df_silver =df_bronze.dropDuplicates(['product_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ###Title case fix

# COMMAND ----------



df_silver = df_bronze.withColumn(
    "category",
    F.when(
        F.col("category").isNull(),
        F.lit(None)
    ).otherwise(
        F.initcap(F.col("category"))
    )
) 

# COMMAND ----------

# MAGIC %md
# MAGIC ###Fixing the spelling Error

# COMMAND ----------

patterns = "protien|protin|protine"

for col_name in ["product_name", "category"]:
    df_silver = df_bronze.withColumn(
        col_name,
        F.initcap(
            F.regexp_replace(
                F.lower(F.col(col_name)),
                patterns,
                "protein"
            )
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardizing the column names to match the parent

# COMMAND ----------

df_silver = df_bronze.withColumn(
    "division",
    F.when(F.col("category") == "Energy Bars", "Nutrition Bars")
     .when(F.col("category") == "Protein Bars", "Nutrition Bars")
     .when(F.col("category") == "Granola & Cereals", "Breakfast Foods")
     .when(F.col("category") == "Recovery Dairy", "Dairy & Recovery")
     .when(F.col("category") == "Healthy Snacks", "Healthy Snacks")
     .when(F.col("category") == "Electrolyte Mix", "Hydration & Electrolytes")
     .otherwise("Other")
).withColumn(
    "variant",
    F.regexp_extract(F.col("product_name"), r"\((.*?)\)", 1)
).withColumn(
    "product_code",
    F.sha2(F.col("product_name").cast("string"), 256)
).withColumn(
    "product_id",
    F.when(
        F.col("product_id").cast("string").rlike("^[0-9]+$"),
        F.col("product_id").cast("string")
    ).otherwise("999999")
).withColumnRenamed("product_name", "product")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Reorder the column names

# COMMAND ----------

print(df_silver.columns)

# COMMAND ----------

df_silver = df_silver.select(
    "product_code",
    "product_id",
    "product",
    "category",
    "division",
    "variant",
    "read_timestamp",
    "file_name",
    "file_size"
)

# COMMAND ----------

display(df_silver)


# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fmcg.silver.products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

catalog = "fmcg"
schema = "silver"
data_source = "products"

# COMMAND ----------

df_gold = spark.sql(f"SELECT * FROM {catalog}.{schema}.{data_source}") \
    .select(
        "product_code",
        "product_id",
        "division",
        "category",
        "product",
        "variant"
    )

df_gold.show(5)

# COMMAND ----------

catalog = "fmcg"
schema = "gold"
data_source = "products"

# COMMAND ----------

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{schema}.sb_dim_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merging tables 

# COMMAND ----------

# Merging Data source with parent
delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_products")

df_child_products = spark.sql(f"SELECT product_code, division, category, product, variant FROM fmcg.gold.sb_dim_products;")

df_child_products.show(5)

# COMMAND ----------

df_child_products = df_child_products.dropDuplicates(["product_code"])

# COMMAND ----------

#upsert (merge) using product_code as the key
delta_table.alias("target").merge(
    df_child_products.alias("source"),
    "target.product_code = source.product_code"
) \
.whenMatchedUpdate(set = {
    "division"  : "source.division",
    "category"  : "source.category",
    "product"   : "source.product",
    "variant"   : "source.variant"
}) \
.whenNotMatchedInsert(values = {
    "product_code" : "source.product_code",
    "division"     : "source.division",
    "category"     : "source.category",
    "product"      : "source.product",
    "variant"      : "source.variant"
}) \
.execute()

print("Merge completed successfully!")

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog `fmcg`; select * from `gold`.`dim_products` ;