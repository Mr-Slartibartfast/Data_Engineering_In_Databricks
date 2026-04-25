# Bronze: raw ingestion
bronze_df = (
    spark.readStream
    .format("cloudFiles")  # Databricks Auto Loader
    .option("cloudFiles.format", "csv")
    .option("header", True)
    .load("/mnt/raw-data/")
)

(
    bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/bronze")
    .outputMode("append")
    .table("bronze.raw_data")
)