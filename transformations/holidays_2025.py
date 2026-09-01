from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
)


HOLIDAY_SCHEMA = StructType(
    [
        StructField("holiday_date", DateType(), nullable=False),
        StructField("holiday_name", StringType(), nullable=False),
        StructField("holiday_type", StringType(), nullable=False),
    ]
)


HOLIDAY_ROWS = [
    ("2025-01-01", "New Years", "Actual"),
    ("2025-01-02", "New Years", "Residual"),
    ("2025-01-03", "New Years", "Residual"),
    ("2025-01-20", "Martin Luther King Jr. Day", "Actual"),
    ("2025-01-21", "Martin Luther King Jr. Day", "Residual"),
    ("2025-02-17", "Washingtons Birthday", "Actual"),
    ("2025-02-18", "Washingtons Birthday", "Residual"),
    ("2025-03-31", "Cesar Chavez Day", "Actual"),
    ("2025-04-01", "Cesar Chavez Day", "Residual"),
    ("2025-04-18", "Good Friday", "Residual"),
    ("2025-04-18", "Good Friday", "Actual"),
    ("2025-05-23", "Memorial Day", "Residual"),
    ("2025-05-26", "Memorial Day", "Actual"),
    ("2025-05-27", "Memorial Day", "Residual"),
    ("2025-06-18", "Juneteenth", "Residual"),
    ("2025-06-19", "Juneteenth", "Actual"),
    ("2025-06-20", "Juneteenth", "Residual"),
    ("2025-06-30", "Independence Day", "Residual"),
    ("2025-07-01", "Independence Day", "Residual"),
    ("2025-07-02", "Independence Day", "Residual"),
    ("2025-07-03", "Independence Day", "Residual"),
    ("2025-07-04", "Independence Day", "Actual"),
    ("2025-09-01", "Labor Day", "Actual"),
    ("2025-09-02", "Labor Day", "Residual"),
    ("2025-10-13", "Columbus Day", "Actual"),
    ("2025-10-14", "Columbus Day", "Residual"),
    ("2025-11-10", "Veterans Day", "Residual"),
    ("2025-11-11", "Veterans Day", "Actual"),
    ("2025-11-12", "Veterans Day", "Residual"),
    ("2025-11-24", "Thanksgiving", "Residual"),
    ("2025-11-25", "Thanksgiving", "Residual"),
    ("2025-11-26", "Thanksgiving", "Residual"),
    ("2025-11-27", "Thanksgiving", "Residual"),
    ("2025-11-28", "Thanksgiving", "Actual"),
    ("2025-12-22", "Christmas Eve", "Residual"),
    ("2025-12-23", "Christmas Eve", "Residual"),
    ("2025-12-24", "Christmas Eve", "Actual"),
    ("2025-12-25", "Christmas", "Actual"),
    ("2025-12-26", "Christmas", "Observed"),
    ("2025-12-29", "New Years", "Residual"),
    ("2025-12-30", "New Years", "Residual"),
    ("2025-12-31", "New Years", "Residual"),
]


@dp.materialized_view(
    name="ref_pems_holidays",
    comment="Holiday and residual travel dates excluded from PeMS analysis",
    schema=HOLIDAY_SCHEMA,
)
def create_pems_holidays():
    string_schema = """
        holiday_date_string STRING,
        holiday_name STRING,
        holiday_type STRING
    """

    return (
        spark.createDataFrame(
            HOLIDAY_ROWS,
            schema=string_schema,
        )
        .select(
            F.to_date("holiday_date_string").alias("holiday_date"),
            F.col("holiday_name"),
            F.col("holiday_type"),
        )
    )