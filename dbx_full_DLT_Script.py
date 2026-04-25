import dlt
from pyspark.sql.functions import col, when, current_timestamp, count

# -----------------------------
# Config (adjust paths)
# -----------------------------
SOURCE_PATH = "/mnt/raw-data/"

# -----------------------------
# Bronze Layer (Raw Ingestion)
# -----------------------------
@dlt.table(
    name="bronze_raw",
    comment="Raw ingested data from source files",
    table_properties={"quality": "bronze"}
)
def bronze_raw():
    return (
        spark.readStream
        .format("cloudFiles")  # Auto Loader
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(SOURCE_PATH)
        .withColumn("ingestion_timestamp", current_timestamp())
    )

# -----------------------------
# Silver Layer (Clean + Validate)
# -----------------------------
@dlt.table(
    name="silver_cleaned",
    comment="Cleaned and validated data",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_age", "age IS NOT NULL AND age > 0")
@dlt.expect("valid_name", "name IS NOT NULL")
def silver_cleaned():
    df = dlt.read("bronze_raw")

    # Approximate median for missing age
    median_age = df.approxQuantile("age", [0.5], 0.01)[0]

    return (
        df.dropDuplicates()
        .fillna({
            "name": "Unknown",
            "age": median_age
        })
        .withColumn("age", col("age").cast("int"))
        .withColumn(
            "age_group",
            when(col("age") < 18, "child")
            .when(col("age") < 35, "young_adult")
            .when(col("age") < 60, "adult")
            .otherwise("senior")
        )
    )

# -----------------------------
# Gold Layer (Business Aggregates)
# -----------------------------
@dlt.table(
    name="gold_user_summary",
    comment="Aggregated user counts by age group",
    table_properties={"quality": "gold"}
)
def gold_user_summary():
    return (
        dlt.read("silver_cleaned")
        .groupBy("age_group")
        .agg(count("*").alias("total_users"))
    )