from pyspark import pipelines as dp
from pyspark.sql import functions as F


def weighted_period_average(
    count_column: str,
    samples_column: str,
    periods_column: str,
    expected_periods: int,
    output_name: str,
):
    valid_period = (
        (F.col(periods_column) == expected_periods)
        & F.col(count_column).isNotNull()
        & F.col(samples_column).isNotNull()
        & (F.col(samples_column) > 0)
    )

    weighted_sum = F.sum(
        F.when(
            valid_period,
            F.col(count_column) * F.col(samples_column),
        )
    )

    total_weight = F.sum(
        F.when(
            valid_period,
            F.col(samples_column),
        )
    )

    return (
        F.round(
            weighted_sum
            / F.when(total_weight > 0, total_weight),
            0,
        )
        .cast("long")
        .alias(output_name)
    )

@dp.materialized_view(
    name="gold_pems_weekday_counts_vsc",
    comment=(
        "Sample-weighted average nonholiday weekday traffic counts "
        "by station for September and October 2025"
    ),
    table_properties={
        "quality": "gold"
    },
)
def create_gold_pems_weekday_counts():

    silver = (
        spark.read.table("silver_pems_weekday_counts_vsc")
        .filter(F.col("year") == 2025)
        .filter(F.col("month_number").isin(9, 10))
    )

    return (
        silver
        .groupBy(
            "year",
            "station",
            "district",
            "freeway",
            "direction_of_travel",
            "lane_type",
            "station_length",
        )
        .agg(
            weighted_period_average(
                "count_day",
                "samples_day",
                "periods_day",
                288,
                "count_day",
            ),

            weighted_period_average(
                "count_ea",
                "samples_ea",
                "periods_ea",
                36,
                "count_ea",
            ),

            weighted_period_average(
                "count_am",
                "samples_am",
                "periods_am",
                36,
                "count_am",
            ),

            weighted_period_average(
                "count_md",
                "samples_md",
                "periods_md",
                78,
                "count_md",
            ),

            weighted_period_average(
                "count_pm",
                "samples_pm",
                "periods_pm",
                42,
                "count_pm",
            ),

            weighted_period_average(
                "count_ev",
                "samples_ev",
                "periods_ev",
                96,
                "count_ev",
            ),

            F.countDistinct("service_date").alias(
                "number_of_weekdays"
            ),

            F.min("service_date").alias(
                "first_service_date"
            ),

            F.max("service_date").alias(
                "last_service_date"
            ),
        )
        .withColumn(
            "analysis_period",
            F.lit("September-October 2025"),
        )
        .select(
            "year",
            "analysis_period",
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
            "number_of_weekdays",
            "first_service_date",
            "last_service_date",
        )
    )