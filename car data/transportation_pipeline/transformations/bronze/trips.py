from pyspark.sql.functions import col, current_timestamp
from pyspark import pipelines as dp

Source_path = "s3://goodcabs-vishal-raw/data-store/trips"

@dp.table(
    name="transportation.bronze.trips",
    comment="Trips raw data (bronze layer)",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def trips_bronze():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFilesPerTrigger", 100)
        .load(Source_path)
    )

    # Clean column name
    df = df.withColumnRenamed(
        "distance_travelled(km)",
        "distance_travelled_km"
    )

    # Metadata columns
    df = (
        df.withColumn("file_name", col("_metadata.file_path"))
          .withColumn("ingest_datetime", current_timestamp())
    )

    return df
