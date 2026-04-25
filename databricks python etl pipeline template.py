# Databricks notebook or .py job

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()

INPUT_PATH = "/FileStore/data/raw_data.csv"
OUTPUT_TABLE = "default.cleaned_data"

# -----------------------------
# Extract
# -----------------------------
def extract(path):
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )
    print(f"Loaded {df.count()} rows")
    return df

# -----------------------------
# Transform
# -----------------------------
def transform(df):
    # Drop duplicates
    df = df.dropDuplicates()

    # Handle missing values
    df = df.fillna({
        "name": "Unknown"
    })

    # Fill age with median (approximation in Spark)
    median_age = df.approxQuantile("age", [0.5], 0.01)[0]
    df = df.fillna({"age": median_age})

    # Convert types
    df = df.withColumn("age", col("age").cast(IntegerType()))

    # Create age groups
    df = df.withColumn(
        "age_group",
        when(col("age") < 18, "child")
        .when((col("age") >= 18) & (col("age") < 35), "young_adult")
        .when((col("age") >= 35) & (col("age") < 60), "adult")
        .otherwise("senior")
    )

    return df

# -----------------------------
# Load (Delta Lake)
# -----------------------------
def load(df, table_name):
    (
        df.write
        .format("delta")
        .mode("overwrite")  # use "append" for incremental loads
        .saveAsTable(table_name)
    )
    print(f"Loaded data into {table_name}")

# -----------------------------
# Run Pipeline
# -----------------------------
df = extract(INPUT_PATH)
df_clean = transform(df)
load(df_clean, OUTPUT_TABLE)