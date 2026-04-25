from pyspark.sql.functions import col, when

silver_df = spark.readStream.table("bronze.raw_data")

# Clean + transform
silver_df = (
    silver_df
    .dropDuplicates()
    .fillna({"name": "Unknown"})
)

# Handle age
median_age = silver_df.approxQuantile("age", [0.5], 0.01)[0]

silver_df = silver_df.fillna({"age": median_age})

silver_df = silver_df.withColumn(
    "age_group",
    when(col("age") < 18, "child")
    .when((col("age") < 35), "young_adult")
    .when((col("age") < 60), "adult")
    .otherwise("senior")
)

(
    silver_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/silver")
    .outputMode("append")
    .table("silver.cleaned_data")
)