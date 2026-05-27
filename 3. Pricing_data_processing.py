# Databricks notebook source
from delta.tables import DeltaTable 
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ###Loading utilities and Initializing widgets

# COMMAND ----------

# MAGIC %run /Workspace/Users/ivychemtai99@gmail.com/Consolidated_Pipeline/Utilities

# COMMAND ----------

base_path = "s3://sportsbar-childdata/gross_price/"

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("schema", "bronze", "Schema")
dbutils.widgets.text("data_source", "gross_price", "Data Source")
dbutils.widgets.text("file_path", "s3://sportsbar-childdata/gross_price/", "File Path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze
# MAGIC

# COMMAND ----------

df_bronze= (
    spark .read.format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .load("s3://sportsbar-childdata/gross_price/") \
  .withColumn("read_timestamp", F.current_timestamp()) \
  .select("*","_metadata.file_name","_metadata.file_size")
)


# COMMAND ----------

df_bronze.display()

# COMMAND ----------

df_bronze.write\
    .mode("overwrite")\
    .format("delta")\
    .option("delta.enableChangeDataFeed", "true")\
    .saveAsTable(f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}.{dbutils.widgets.get('data_source')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver

# COMMAND ----------

df_silver = spark.table("fmcg.bronze.gross_price")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Normalize month field

# COMMAND ----------

# DBTITLE 1,Cell 24
# Step 1: Apply all silver transformations together
df_silver = df_bronze \
    .withColumn(
        "month",
        F.coalesce(
            F.expr("try_to_date(month, 'dd/MM/yyyy')"),
            F.expr("try_to_date(month, 'yyyy-MM-dd')"),
            F.expr("try_to_date(month, 'yyyy/MM/dd')"),
            F.expr("try_to_date(month, 'dd-MM-yyyy')"),
            F.lit("1900-01-01").cast("date")
        )
    ) \
    .withColumn(
        "gross_price",
        F.when(F.col("gross_price").rlike(r'^\-?\d+(\.\d+)?$'),
            F.when(F.col("gross_price").cast("double") < 0, -1 * F.col("gross_price").cast("double"))
             .otherwise(F.col("gross_price").cast("double"))
        )
        .otherwise(0)
    )

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Handling gross price values

# COMMAND ----------

# MAGIC %md
# MAGIC ###Joning to the product table
# MAGIC

# COMMAND ----------

df_products = spark.table("fmcg.silver.products")

df_joined = df_silver.join(df_products.select("product_id", "product_code"), on="product_id", how="inner")

df_joined = df_joined.select(
    "product_id",
    "product_code",
    "month",
    "gross_price",
    "read_timestamp",
    "file_name",
    "file_size"
)

df_joined.display()




# COMMAND ----------

# MAGIC %md
# MAGIC ###Writting the joined table to the silver table

# COMMAND ----------

df_joined.write \
    .option("delta.enableChangeDataFeed", "true") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}.{dbutils.widgets.get('data_source')}_joined")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS fmcg.bronze.gross_price_joined")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Gold

# COMMAND ----------

df_gold=df_joined.select("product_code","month","gross_price")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
gold_schema = dbutils.widgets.get("schema")
data_source = dbutils.widgets.get("data_source")

df_gold.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Merging with Parent Company

# COMMAND ----------

df_gold_price = spark.table("fmcg.gold.sb_dim_gross_price")
df_gold_price.show(5)

# COMMAND ----------

df_gold_price = (
    df_gold_price
    .withColumn("year", F.year("month"))
    # 0 = non-zero price, 1 = zero price → non-zero comes first
    .withColumn("is_zero", F.when(F.col("gross_price") == 0, 1).otherwise(0))
)

w = (
    Window
    .partitionBy("product_code", "year")
    .orderBy(F.col("is_zero"), F.col("month").desc())
)

df_gold_latest_price = (
    df_gold_price
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") == 1)
)

# COMMAND ----------

display(df_gold_latest_price)


# COMMAND ----------

## Take required cols
df_gold_latest_price = df_gold_latest_price \
    .select("product_code", "year", "gross_price") \
    .withColumnRenamed("gross_price", "price_inr") \
    .select("product_code", "price_inr", "year")

# change year to string
df_gold_latest_price = df_gold_latest_price \
    .withColumn("year", F.col("year").cast("string"))

df_gold_latest_price.show(5)

# COMMAND ----------

# DBTITLE 1,Cell 28
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_gross_price")

delta_table.alias("target").merge(
    source=df_gold_latest_price.alias("source"),
    condition="target.product_code = source.product_code"
).whenMatchedUpdate(
    set={
        "price_inr": "source.price_inr",
        "year"     : "source.year"
    }
).whenNotMatchedInsert(
    values={
        "product_code": "source.product_code",
        "price_inr"   : "source.price_inr",
        "year"        : "source.year"
    }
).execute()

print("Merge completed successfully!")

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {catalog}.{gold_schema}.gross_price_joined")
print("Table dropped successfully!")