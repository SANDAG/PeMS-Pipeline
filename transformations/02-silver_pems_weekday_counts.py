from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="silver_pems_weekday_counts",
    comment=(
        "Complete nonholiday weekday PeMS counts by station and "
        "3 AM-to-3 AM service date for September and October 2025"
    ),
    table_properties={
        "quality": "silver",
        "delta.feature.timestampNtz": "supported"
    },
)
@dp.expect_or_drop(
    "station_is_present",
    "station IS NOT NULL",
)
@dp.expect_or_drop(
    "complete_day",
    "periods_day = 288",
)
@dp.expect_or_drop(
    "complete_ea",
    "periods_ea = 36",
)
@dp.expect_or_drop(
    "complete_am",
    "periods_am = 36",
)
@dp.expect_or_drop(
    "complete_md",
    "periods_md = 78",
)
@dp.expect_or_drop(
    "complete_pm",
    "periods_pm = 42",
)
@dp.expect_or_drop(
    "complete_ev",
    "periods_ev = 96",
)
def create_silver_pems_weekday_counts():

    # -------------------------------------------------------------------------
    # Read required station-level columns from bronze.
    # -------------------------------------------------------------------------

    bronze = (
        spark.read.table("bronze_raw_pems")
        .select(
            F.col("timestamp"),
            F.col("station"),
            F.col("district"),
            F.col("freeway"),
            F.col("direction_of_travel"),
            F.col("lane_type"),
            F.col("station_length"),
            F.col("samples"),
            F.col("percent_observed"),
            F.col("total_flow"),
        )
        .filter(F.col("timestamp").isNotNull())
        .filter(F.col("station").isNotNull())
        .filter(F.col("samples") > 0)
        .filter(F.col("total_flow").isNotNull())
    )

    # -------------------------------------------------------------------------
    # Create time-of-day values.
    #
    # minute_of_day:
    #   03:00 = 180
    #   06:00 = 360
    #   09:00 = 540
    #   15:30 = 930
    #   19:00 = 1140
    # -------------------------------------------------------------------------

    timed = (
        bronze
        .withColumn(
            "calendar_date",
            F.to_date("timestamp"),
        )
        .withColumn(
            "minute_of_day",
            F.hour("timestamp") * F.lit(60)
            + F.minute("timestamp"),
        )
    )

    # -------------------------------------------------------------------------
    # Assign midnight-2:55 AM records to the previous service date.
    #
    # For example:
    #   2025-09-04 01:00 belongs to service_date 2025-09-03.
    # -------------------------------------------------------------------------

    timed = (
        timed
        .withColumn(
            "service_date",
            F.when(
                F.col("minute_of_day") < 180,
                F.date_sub(F.col("calendar_date"), 1),
            ).otherwise(
                F.col("calendar_date")
            ),
        )
        .withColumn(
            "year",
            F.year("service_date"),
        )
        .withColumn(
            "month_number",
            F.month("service_date"),
        )
        .withColumn(
            "month",
            F.date_format("service_date", "MMMM"),
        )
        .withColumn(
            "day_of_week",
            F.date_format("service_date", "EEEE"),
        )
)

    # -------------------------------------------------------------------------
    # Assign each five-minute record to one time period.
    #
    # EA: 03:00 <= time < 06:00
    # AM: 06:00 <= time < 09:00
    # MD: 09:00 <= time < 15:30
    # PM: 15:30 <= time < 19:00
    # EV: 19:00 <= time OR time < 03:00
    # -------------------------------------------------------------------------

    periodized = (
        timed
        .withColumn(
            "time_period",
            F.when(
                (F.col("minute_of_day") >= 180)
                & (F.col("minute_of_day") < 360),
                F.lit("EA"),
            )
            .when(
                (F.col("minute_of_day") >= 360)
                & (F.col("minute_of_day") < 540),
                F.lit("AM"),
            )
            .when(
                (F.col("minute_of_day") >= 540)
                & (F.col("minute_of_day") < 930),
                F.lit("MD"),
            )
            .when(
                (F.col("minute_of_day") >= 930)
                & (F.col("minute_of_day") < 1140),
                F.lit("PM"),
            )
            .otherwise(
                F.lit("EV")
            ),
        )
    )

    # -------------------------------------------------------------------------
    # Keep September and October 2025 service dates.
    #
    # Spark dayofweek:
    #   Sunday = 1
    #   Saturday = 7
    # -------------------------------------------------------------------------

    valid_weekdays = (
        periodized
        .filter(F.col("year") == 2025)
        .filter(F.col("month_number").isin(9, 10))
        .filter(
            ~F.dayofweek("service_date").isin(1, 7)
        )
    )

    # -------------------------------------------------------------------------
    # Read unique holiday exclusion dates.
    #
    # Holidays are matched to service_date, not the raw calendar date.
    # -------------------------------------------------------------------------

    holiday_dates = (
        spark.read.table("ref_pems_holidays")
        .select("holiday_date")
        .dropDuplicates(["holiday_date"])
    )

    nonholiday_records = (
        valid_weekdays.alias("p")
        .join(
            holiday_dates.alias("h"),
            F.col("p.service_date")
            == F.col("h.holiday_date"),
            how="left_anti",
        )
    )

    # -------------------------------------------------------------------------
    # Aggregate to one service date and station.
    #
    # Counts are sums of five-minute total_flow.
    # Samples are retained for optional gold-layer weighting.
    # -------------------------------------------------------------------------

    station_day_counts = (
        nonholiday_records
        .groupBy(
            "service_date",
            "year",
            "month",
            "month_number",
            "day_of_week",
            "station",
            "district",
            "freeway",
            "direction_of_travel",
            "lane_type",
            "station_length",
        )
        .agg(
            # -------------------------------------------------------------
            # Traffic counts
            # -------------------------------------------------------------

            F.sum("total_flow").alias("count_day"),

            F.sum(
                F.when(
                    F.col("time_period") == "EA",
                    F.col("total_flow"),
                ).otherwise(0.0)
            ).alias("count_ea"),

            F.sum(
                F.when(
                    F.col("time_period") == "AM",
                    F.col("total_flow"),
                ).otherwise(0.0)
            ).alias("count_am"),

            F.sum(
                F.when(
                    F.col("time_period") == "MD",
                    F.col("total_flow"),
                ).otherwise(0.0)
            ).alias("count_md"),

            F.sum(
                F.when(
                    F.col("time_period") == "PM",
                    F.col("total_flow"),
                ).otherwise(0.0)
            ).alias("count_pm"),

            F.sum(
                F.when(
                    F.col("time_period") == "EV",
                    F.col("total_flow"),
                ).otherwise(0.0)
            ).alias("count_ev"),

            # -------------------------------------------------------------
            # Sample totals for later weighting
            # -------------------------------------------------------------

            F.sum("samples").alias("samples_day"),

            F.sum(
                F.when(
                    F.col("time_period") == "EA",
                    F.col("samples"),
                ).otherwise(0.0)
            ).alias("samples_ea"),

            F.sum(
                F.when(
                    F.col("time_period") == "AM",
                    F.col("samples"),
                ).otherwise(0.0)
            ).alias("samples_am"),

            F.sum(
                F.when(
                    F.col("time_period") == "MD",
                    F.col("samples"),
                ).otherwise(0.0)
            ).alias("samples_md"),

            F.sum(
                F.when(
                    F.col("time_period") == "PM",
                    F.col("samples"),
                ).otherwise(0.0)
            ).alias("samples_pm"),

            F.sum(
                F.when(
                    F.col("time_period") == "EV",
                    F.col("samples"),
                ).otherwise(0.0)
            ).alias("samples_ev"),

            # -------------------------------------------------------------
            # Completeness counts
            # -------------------------------------------------------------

            F.countDistinct("timestamp").alias("periods_day"),

            F.countDistinct(
                F.when(
                    F.col("time_period") == "EA",
                    F.col("timestamp"),
                )
            ).alias("periods_ea"),

            F.countDistinct(
                F.when(
                    F.col("time_period") == "AM",
                    F.col("timestamp"),
                )
            ).alias("periods_am"),

            F.countDistinct(
                F.when(
                    F.col("time_period") == "MD",
                    F.col("timestamp"),
                )
            ).alias("periods_md"),

            F.countDistinct(
                F.when(
                    F.col("time_period") == "PM",
                    F.col("timestamp"),
                )
            ).alias("periods_pm"),

            F.countDistinct(
                F.when(
                    F.col("time_period") == "EV",
                    F.col("timestamp"),
                )
            ).alias("periods_ev"),

            # -------------------------------------------------------------
            # Observation-quality fields
            # -------------------------------------------------------------

            F.avg("percent_observed").alias(
                "average_percent_observed"
            ),

            F.min("percent_observed").alias(
                "minimum_percent_observed"
            ),

            F.min("timestamp").alias("first_timestamp"),

            F.max("timestamp").alias("last_timestamp"),
        )
    )

    # -------------------------------------------------------------------------
    # Retain only days with complete coverage in every period.
    # -------------------------------------------------------------------------

    return (
        station_day_counts
        .filter(F.col("periods_day") == 288)
        .filter(F.col("periods_ea") == 36)
        .filter(F.col("periods_am") == 36)
        .filter(F.col("periods_md") == 78)
        .filter(F.col("periods_pm") == 42)
        .filter(F.col("periods_ev") == 96)
        .withColumn(
            "analysis_period",
            F.lit("September-October 2025"),
        )
        .select(
            "service_date",
            "year",
            "month",
            "month_number",
            "day_of_week", 
            "station",
            "district",
            "freeway",
            "direction_of_travel",
            "lane_type",
            "station_length",
            "count_day",
            "count_ea",
            "count_am",
            "count_md",
            "count_pm",
            "count_ev",
            "samples_day",
            "samples_ea",
            "samples_am",
            "samples_md",
            "samples_pm",
            "samples_ev",
            "periods_day",
            "periods_ea",
            "periods_am",
            "periods_md",
            "periods_pm",
            "periods_ev",
            "average_percent_observed",
            "minimum_percent_observed",
            "first_timestamp",
            "last_timestamp",
        )
    )