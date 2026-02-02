from pyspark.sql.functions import col, current_timestamp
from pyspark import pipelines as dp
Source_path = "s3://goodcabs-vishal-raw/data-store/city"

@dp.materialized_view(
    name="transportation.bronze.city",
    comment="City Raw Data Processing",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_bronze():
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "current_record")
        .load(Source_path)
    )

    df = (
        df.withColumn("ingest_datetime", current_timestamp())
    )

    return df
