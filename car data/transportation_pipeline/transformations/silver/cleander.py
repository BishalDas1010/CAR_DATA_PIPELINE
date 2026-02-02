from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Safe parameter defaults (works in DLT)
START_DATE = spark.conf.get("start_date", "2025-01-01")
END_DATE   = spark.conf.get("end_date", "2025-12-31")

# Ensure valid date range
if START_DATE > END_DATE:
    START_DATE, END_DATE = END_DATE, START_DATE

@dp.materialized_view(
    name="transportation.silver.calendar",
    comment="Calendar dimension with comprehensive date attributes and Indian holidays",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def calendar():

    # ---------------------------
    # Generate date range
    # ---------------------------
    df = spark.sql(f"""
        SELECT explode(
            sequence(
                to_date('{START_DATE}'),
                to_date('{END_DATE}'),
                interval 1 day
            )
        ) AS date
    """)

    # ---------------------------
    # Date key (YYYYMMDD)
    # ---------------------------
    df = df.withColumn(
        "date_key",
        F.date_format("date", "yyyyMMdd").cast("int")
    )

    # ---------------------------
    # Core date attributes
    # ---------------------------
    df = (
        df.withColumn("year", F.year("date"))
          .withColumn("month", F.month("date"))
          .withColumn("quarter", F.quarter("date"))
          .withColumn("day_of_month", F.dayofmonth("date"))
          .withColumn("day_of_week", F.date_format("date", "EEEE"))
          .withColumn("day_of_week_abbr", F.date_format("date", "EEE"))
          .withColumn("day_of_week_num", F.dayofweek("date"))  # 1=Sun, 7=Sat
    )

    # ---------------------------
    # Month / Quarter labels
    # ---------------------------
    df = (
        df.withColumn("month_name", F.date_format("date", "MMMM"))
          .withColumn("month_year", F.concat_ws(" ", F.col("month_name"), F.col("year")))
          .withColumn("quarter_year", F.concat_ws(" ", F.concat(F.lit("Q"), F.col("quarter")), F.col("year")))
    )

    # ---------------------------
    # Week & year attributes
    # ---------------------------
    df = (
        df.withColumn("week_of_year", F.weekofyear("date"))
          .withColumn("day_of_year", F.dayofyear("date"))
    )

    # ---------------------------
    # Weekend / weekday flags
    # ---------------------------
    df = (
        df.withColumn("is_weekend", F.col("day_of_week_num").isin(1, 7))
          .withColumn("is_weekday", ~F.col("day_of_week_num").isin(1, 7))
    )

    # ---------------------------
    # Indian fixed holidays
    # ---------------------------
    df = (
        df.withColumn(
            "holiday_name",
            F.when((F.col("month") == 1) & (F.col("day_of_month") == 26), "Republic Day")
             .when((F.col("month") == 8) & (F.col("day_of_month") == 15), "Independence Day")
             .when((F.col("month") == 10) & (F.col("day_of_month") == 2), "Gandhi Jayanti")
        )
        .withColumn("is_holiday", F.col("holiday_name").isNotNull())
    )

    # ---------------------------
    # Silver metadata
    # ---------------------------
    df = df.withColumn(
        "silver_processed_timestamp",
        F.current_timestamp()
    )

    # ---------------------------
    # Final column order
    # ---------------------------
    return df.select(
        "date",
        "date_key",
        "year",
        "month",
        "month_name",
        "month_year",
        "quarter",
        "quarter_year",
        "week_of_year",
        "day_of_year",
        "day_of_month",
        "day_of_week",
        "day_of_week_abbr",
        "is_weekday",
        "is_weekend",
        "is_holiday",
        "holiday_name",
        "silver_processed_timestamp"
    )
