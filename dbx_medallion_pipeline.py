import dlt
from pyspark.sql.functions import col, when

# -----------------------------
# Bronze
# -----------------------------
@dlt.table
def bronze_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", True)
        .load("/mnt/raw-data/")
    )

# -----------------------------
# Silver
# -----------------------------
@dlt.table
@dlt.expect("valid_age", "age IS NOT NULL")
def silver_cleaned():
    df = dlt.read("bronze_raw")

    median_age = df.approxQuantile("age", [0.5], 0.01)[0]

    return (
        df.dropDuplicates()
        .fillna({"name": "Unknown", "age": median_age})
        .withColumn(
            "age_group",
            when(col("age") < 18, "child")
            .when(col("age") < 35, "young_adult")
            .when(col("age") < 60, "adult")
            .otherwise("senior")
        )
    )

# -----------------------------
# Gold
# -----------------------------
@dlt.table
def gold_summary():
    return (
        dlt.read("silver_cleaned")
        .groupBy("age_group")
        .count()
    )