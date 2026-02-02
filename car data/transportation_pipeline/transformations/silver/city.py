from pyspark.sql import functions as f
from pyspark import pipelines as dp
@dp.materialized_view(
    name="transportation.silver.city",
    comment="This is silver table",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_silver():
    df_bronze = spark.read.table("transportation.bronze.city")
    df_silver = df_bronze.select(
        f.col("city_id").alias("city_id"),
        f.col("city_name").alias("city_name"),
        f.col("ingest_datetime").alias("bronze_ingest_timestap")
    )
    df_silver=df_silver.withColumn(
        "silver_processes_timestamp",f.current_timestamp()
    )
    return df_silver