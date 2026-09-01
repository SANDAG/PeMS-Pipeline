from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="silver_pems_quality",
    comment=(
        "PeMS station-day quality assessment based on completeness "
        "of five-minute observations by analysis period"
    ),
    table_properties={
        "quality": "silver",
        "delta.feature.timestampNtz": "supported",
    },
)
def create_silver_pems_quality():

    station_days = spark.read.table("silver_pems_station_days")

    return (
        station_days

        # ------------------------------------------------------------------
        # Completeness flags
        # ------------------------------------------------------------------

        .withColumn(
            "qc_complete_day",
            F.col("periods_day") == 288,
        )
        .withColumn(
            "qc_complete_ea",
            F.col("periods_ea") == 36,
        )
        .withColumn(
            "qc_complete_am",
            F.col("periods_am") == 36,
        )
        .withColumn(
            "qc_complete_md",
            F.col("periods_md") == 78,
        )
        .withColumn(
            "qc_complete_pm",
            F.col("periods_pm") == 42,
        )
        .withColumn(
            "qc_complete_ev",
            F.col("periods_ev") == 96,
        )

        # ------------------------------------------------------------------
        # Overall QC result
        # ------------------------------------------------------------------

        .withColumn(
            "qc_pass",
            F.col("qc_complete_day")
            & F.col("qc_complete_ea")
            & F.col("qc_complete_am")
            & F.col("qc_complete_md")
            & F.col("qc_complete_pm")
            & F.col("qc_complete_ev"),
        )

        # ------------------------------------------------------------------
        # Helpful completeness metric for reporting
        # ------------------------------------------------------------------

        .withColumn(
            "percent_periods_complete",
            F.col("periods_day") / F.lit(288.0) * 100.0,
        )
    )