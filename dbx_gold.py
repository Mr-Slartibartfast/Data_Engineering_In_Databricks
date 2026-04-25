from pyspark.sql.functions import count

gold_df = (
    spark.readStream.table("silver.cleaned_data")
    .groupBy("age_group")
    .agg(count("*").alias("total_users"))
)

(
    gold_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/gold")
    .outputMode("complete")
    .table("gold.user_summary")
)