# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import trim, col
from pyspark.sql.window import Window


# COMMAND ----------

# MAGIC %run /Workspace/Users/ivychemtai99@gmail.com/Consolidated_Pipeline/Utilities

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f"s3://sportsbar-childdata/{data_source}/*csv"

print(base_path)


# COMMAND ----------

# MAGIC %md
# MAGIC ##data frame

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

df.write.format("delta") \
.mode("overwrite") \
.saveAsTable(f"{catalog}.bronze.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver processing 

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

df_bronze = spark.read.table(f"{catalog}.bronze.{data_source}")



# COMMAND ----------

# MAGIC %md
# MAGIC ##data cleaning

# COMMAND ----------

# MAGIC %md
# MAGIC ###dropping duplicates

# COMMAND ----------

df_silver = (
    df_bronze
    .dropDuplicates()
    .dropna()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trimming

# COMMAND ----------

df_silver  = df_bronze.withColumn(
    "customer_name",
    trim(col("customer_name"))
)

# COMMAND ----------

city_mapping = {
    "Bengaluru": "Bengaluru",
    "Bengalore": "Bengaluru",

    "Hyderabadd": "Hyderabad",
    "Hyderabad": "Hyderabad",

    "NewDehli": "New Delhi",
    "NewDelhi": "New Delhi",
    "NewDelhee": "New Delhi"
}
allowed = {"Bengaluru", "Hyderabad", "New Delhi"}

df_silver = (
    df_bronze
    .withColumn(
        "city",
        F.when(
            F.col("city").isin(allowed),
            F.col("city")
        ).otherwise(None)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Case formatting
# MAGIC

# COMMAND ----------

df_silver = (
    df_bronze
    .withColumn(
        "customer_name",
        F.initcap(F.col("customer_name"))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Getting rid of null city names

# COMMAND ----------

df_null = df.filter(F.col("city").isNull()) \
            .select("customer_name") \
            .distinct()

# COMMAND ----------

df_lookup = (
    df.filter(F.col("city").isNotNull())
      .select("customer_name", "city")
      .dropDuplicates(["customer_name"])
)

# COMMAND ----------

df_silver = (
    df.alias("a")
    .join(
        df_lookup.alias("b"),
        on="customer_name",
        how="left"
    )
    .select(
        F.col("a.customer_name").alias("customer_name"),
        F.coalesce(
            F.col("a.city"),
            F.col("b.city")
        ).alias("city")
    )
)

# COMMAND ----------

df_silver = df_bronze.withColumn(
    "customer_id",
    F.col("customer_id").cast("string")
)

# COMMAND ----------

df_silver = (
    df_silver
    # Customer column: CustomerName-City or CustomerName-Unknown
    .withColumn(
        "customer",
        F.concat_ws(
            "-",
            F.col("customer_name"),
            F.coalesce(F.col("city"), F.lit("Unknown"))
        )
    )
    # Add required columns
    .withColumn("platform", F.lit("Sports Bar"))
    .withColumn("market", F.lit("India"))
    .withColumn("channel", F.lit("Acquisition"))
)

# COMMAND ----------

display(df_silver)


# COMMAND ----------

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.silver.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Processing

# COMMAND ----------

df_gold = (
    df_silver.select(
        "customer_id",
        "customer_name",
        "city",
        "customer",
        "market",
        "platform",
        "channel"
    )
)

# COMMAND ----------

df_gold.display()


# COMMAND ----------

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.gold.sb_dim_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merging
# MAGIC

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")

# Source (child customers from silver/gold staging)
df_child_customers = (
    spark.table("fmcg.gold.sb_dim_customers")
    .select(
        F.col("customer_id").alias("customer_code"),
        "customer",
        "market",
        "platform",
        "channel"
    )
)

# COMMAND ----------

# Remove duplicates to ensure 1:1 mapping for merge
df_child_customers = df_child_customers.dropDuplicates(["customer_code"])

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC