# %% [markdown]
# Read-only exploratory summary of the bronze PeMS layer (bronze_raw_pems_vsc).
# Run cell by cell (VS Code: "Run Cell" above each `# %%`).
# Does not write anything back to Databricks; not part of the DLT pipeline.

# %%
from pathlib import Path
import matplotlib.pyplot as plt
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = DatabricksSession.builder.profile("dev").serverless(True).getOrCreate()
bronze = spark.table("travel_data.pems.bronze_raw_pems_vsc")

PLOTS_DIR = Path(__file__).parent / "plots"
PLOTS_DIR.mkdir(exist_ok=True)

print(f"Total rows: {bronze.count():,}")

# %% Stations dropped entirely by silver's samples>0 AND total_flow IS NOT NULL filters 
total_stations = bronze.select("station").distinct().count()
stations_with_surviving_rows = (
    bronze
    .filter((F.col("samples") > 0) & F.col("total_flow").isNotNull())
    .select("station")
    .distinct()
    .count()
)
stations_fully_dropped = total_stations - stations_with_surviving_rows

print(f"Total distinct stations in bronze: {total_stations:,}")
print(
    f"Stations with >=1 row passing samples>0 AND total_flow NOT NULL: "
    f"{stations_with_surviving_rows:,} ({stations_with_surviving_rows / total_stations * 100:.1f}%)"
)
print(
    f"Stations with ZERO surviving rows (fully dropped before silver aggregates): "
    f"{stations_fully_dropped:,} ({stations_fully_dropped / total_stations * 100:.1f}%)"
)
 
# %% Distributions of key raw fields
# these filters are used in the silver layer, so it's useful to see how many rows are dropped by each one
quantile_cols = ["percent_observed", "samples", "total_flow", "avg_occupancy", "avg_speed"]
quantiles = [0.0, 0.05, 0.25, 0.5, 0.75, 0.95, 1.0]
rows = [(c, *bronze.approxQuantile(c, quantiles, 0.001)) for c in quantile_cols]
spark.createDataFrame(rows, ["column", "min", "p05", "p25", "median", "p75", "p95", "max"]).show(truncate=False)

bronze.select(
    F.count("*").alias("total_rows"),
    F.sum(F.when(F.col("percent_observed").isNull(), 1).otherwise(0)).alias("null_percent_observed"),
    F.sum(F.when(F.col("total_flow").isNull(), 1).otherwise(0)).alias("null_total_flow"),
    F.sum(F.when(F.col("total_flow") < 0, 1).otherwise(0)).alias("negative_total_flow"),
    F.sum(F.when(F.col("samples") <= 0, 1).otherwise(0)).alias("zero_or_negative_samples"),
).show()

# %% samples (0 vs >0) x total_flow (NULL vs NOT NULL) cross-tab
# Relevant because silver's row-level filters (samples > 0, total_flow IS NOT
# NULL) drop rows independently -- this shows how much each filter catches
# on its own vs. how much overlaps.
samples_flow_pdf = (
    bronze
    .withColumn("samples_cat", F.when(F.col("samples") > 0, F.lit(">0")).otherwise(F.lit("0")))
    .withColumn("total_flow_cat", F.when(F.col("total_flow").isNull(), F.lit("NULL")).otherwise(F.lit("NOT NULL")))
    .groupBy("samples_cat", "total_flow_cat")
    .agg(F.count("*").alias("records"))
    .withColumn("pct_of_total", F.round(F.col("records") * 100.0 / F.sum("records").over(Window.partitionBy()), 2))
    .orderBy("samples_cat", "total_flow_cat")
    .toPandas()
)
print(samples_flow_pdf)

# %% percent_observed bucket distribution + plot
# Thresholds must stay high-to-low for the top-down .when() chain below.
PO_BUCKETS = [(100, "100"), (75, "75-99"), (50, "50-75"), (25, "25-50")]
PO_BUCKET_ORDER = ["0", "<25", "25-50", "50-75", "75-99", "100"]  # small to large, for display

bucket_expr = F.when(F.col("percent_observed").isNull(), F.lit("null"))
for lo, label in PO_BUCKETS:
    bucket_expr = bucket_expr.when(F.col("percent_observed") >= lo, F.lit(label))
bucket_expr = (
    bucket_expr
    .when(F.col("percent_observed") == 0, F.lit("0"))
    .when(F.col("percent_observed") > 0, F.lit("<25"))
    .otherwise(F.lit("negative"))
)

bucket_pdf = (
    bronze.withColumn("po_bucket", bucket_expr)
    .groupBy("po_bucket")
    .agg(F.count("*").alias("records"))
    .withColumn("pct_of_total", F.round(F.col("records") * 100.0 / F.sum("records").over(Window.partitionBy()), 2))
    .toPandas()
    .set_index("po_bucket")
    .reindex(PO_BUCKET_ORDER)
    .fillna(0)
    .reset_index()
)
print(bucket_pdf)

fig, ax = plt.subplots(figsize=(7, 5))
bars = ax.bar(bucket_pdf["po_bucket"], bucket_pdf["pct_of_total"], color="#4C72B0")
ax.set_xlabel("percent_observed bucket")
ax.set_ylabel("% of bronze records")
ax.set_title("Bronze: percent_observed bucket distribution")
ax.set_ylim(0, bucket_pdf["pct_of_total"].max() * 1.25)
labels = [
    f"{int(records):,}\n({pct:.1f}%)"
    for records, pct in zip(bucket_pdf["records"], bucket_pdf["pct_of_total"])
]
ax.bar_label(bars, labels=labels, padding=3)
fig.savefig(PLOTS_DIR / "percent_observed_buckets.png", dpi=150)
plt.show()

# %% Interval availability: % of 288 daily 5-min intervals present, per station-day
# Different from percent_observed above -- this measures whether a bronze row
# exists at all for each 5-min slot, not sensor health within a reported row.
#
# Matches silver_pems_weekday_counts_vsc's population and completeness gate
# exactly: service_date (3AM-3AM, not calendar midnight), nonholiday Sep/Oct
# 2025 weekdays only. So the "100" bucket here = station-days kept in silver;
# everything else = filtered out for incomplete interval coverage.
holiday_dates = (
    spark.read.table("travel_data.pems.ref_pems_holidays_vsc")
    .select("holiday_date")
    .dropDuplicates(["holiday_date"])
)

service_dated = (
    bronze
    .withColumn("calendar_date", F.to_date("timestamp"))
    .withColumn("minute_of_day", F.hour("timestamp") * 60 + F.minute("timestamp"))
    .withColumn(
        "service_date",
        F.when(
            F.col("minute_of_day") < 180,
            F.date_sub(F.col("calendar_date"), 1),
        ).otherwise(F.col("calendar_date")),
    )
    .filter(F.year("service_date") == 2025)
    .filter(F.month("service_date").isin(9, 10))
    .filter(~F.dayofweek("service_date").isin(1, 7))
    .join(holiday_dates, F.col("service_date") == F.col("holiday_date"), how="left_anti")
)

interval_counts = (
    service_dated
    .groupBy("station", "service_date")
    .agg(
        F.countDistinct("timestamp").alias("periods_present"),
        F.avg("percent_observed").alias("avg_percent_observed"),
    )
    .withColumn("pct_intervals_present", F.round(F.col("periods_present") * 100.0 / 288, 2))
)

IA_BUCKETS = [(100, "100"), (75, "75-99"), (50, "50-75"), (25, "25-50")]
IA_BUCKET_ORDER = ["0", "<25", "25-50", "50-75", "75-99", "100"]

ia_expr = F.when(F.col("pct_intervals_present").isNull(), F.lit("null"))
for lo, label in IA_BUCKETS:
    ia_expr = ia_expr.when(F.col("pct_intervals_present") >= lo, F.lit(label))
ia_expr = (
    ia_expr
    .when(F.col("pct_intervals_present") == 0, F.lit("0"))
    .when(F.col("pct_intervals_present") > 0, F.lit("<25"))
    .otherwise(F.lit("negative"))
)

ia_pdf = (
    interval_counts.withColumn("ia_bucket", ia_expr)
    .groupBy("ia_bucket")
    .agg(F.count("*").alias("station_days"))
    .withColumn("pct_of_total", F.round(F.col("station_days") * 100.0 / F.sum("station_days").over(Window.partitionBy()), 2))
    .toPandas()
    .set_index("ia_bucket")
    .reindex(IA_BUCKET_ORDER)
    .fillna(0)
    .reset_index()
)
print(ia_pdf)

fig, ax = plt.subplots(figsize=(7, 5))
bars = ax.bar(ia_pdf["ia_bucket"], ia_pdf["pct_of_total"], color="#8172B2")
ax.set_xlabel("% of 288 daily 5-min intervals present")
ax.set_ylabel("% of station-days")
ax.set_title("Interval availability: nonholiday Sep-Oct 2025 weekday station-days\n(\"100\" = kept in silver, else filtered out for incompleteness)")
ax.set_ylim(0, ia_pdf["pct_of_total"].max() * 1.25)
labels = [
    f"{int(n):,}\n({p:.1f}%)"
    for n, p in zip(ia_pdf["station_days"], ia_pdf["pct_of_total"])
]
ax.bar_label(bars, labels=labels, padding=3)
fig.savefig(PLOTS_DIR / "interval_availability_buckets.png", dpi=150)
plt.show()

# %% Station-day distribution by percent_observed category
# Same population as interval availability above (nonholiday Sep-Oct 2025
# weekday station-days), but bucketed by each station-day's AVERAGE
# percent_observed using the same categories as the raw-record chart.
# "100" = fully observed all day, no imputation; lower buckets = station-days
# relying on increasing amounts of imputed/estimated values.
po_day_expr = F.when(F.col("avg_percent_observed").isNull(), F.lit("null"))
for lo, label in PO_BUCKETS:
    po_day_expr = po_day_expr.when(F.col("avg_percent_observed") >= lo, F.lit(label))
po_day_expr = (
    po_day_expr
    .when(F.col("avg_percent_observed") == 0, F.lit("0"))
    .when(F.col("avg_percent_observed") > 0, F.lit("<25"))
    .otherwise(F.lit("negative"))
)

po_day_pdf = (
    interval_counts.withColumn("po_bucket", po_day_expr)
    .groupBy("po_bucket")
    .agg(F.count("*").alias("station_days"))
    .withColumn("pct_of_total", F.round(F.col("station_days") * 100.0 / F.sum("station_days").over(Window.partitionBy()), 2))
    .toPandas()
    .set_index("po_bucket")
    .reindex(PO_BUCKET_ORDER)
    .fillna(0)
    .reset_index()
)
print(po_day_pdf)

fig, ax = plt.subplots(figsize=(7, 5))
bars = ax.bar(po_day_pdf["po_bucket"], po_day_pdf["pct_of_total"], color="#C44E52")
ax.set_xlabel("percent_observed bucket")
ax.set_ylabel("% of station-days")
ax.set_title("Station-day distribution: fully observed (100) vs. imputed (lower buckets)")
ax.set_ylim(0, po_day_pdf["pct_of_total"].max() * 1.25)
labels = [
    f"{int(n):,}\n({p:.1f}%)"
    for n, p in zip(po_day_pdf["station_days"], po_day_pdf["pct_of_total"])
]
ax.bar_label(bars, labels=labels, padding=3)
fig.savefig(PLOTS_DIR / "station_day_percent_observed_buckets.png", dpi=150)
plt.show()

# %% How many stations survive if we only keep station-days with avg_percent_observed == 100?
total_stations = interval_counts.select("station").distinct().count()
stations_with_clean_day = (
    interval_counts.filter(F.col("avg_percent_observed") == 100)
    .select("station")
    .distinct()
    .count()
)
stations_with_zero_clean_days = total_stations - stations_with_clean_day

print(f"Total stations in population: {total_stations:,}")
print(
    f"Stations with >=1 fully-observed (100%) station-day: "
    f"{stations_with_clean_day:,} ({stations_with_clean_day / total_stations * 100:.1f}%)"
)
print(
    f"Stations with ZERO fully-observed station-days (fully dropped): "
    f"{stations_with_zero_clean_days:,} ({stations_with_zero_clean_days / total_stations * 100:.1f}%)"
)

# %% Daily average percent_observed trend + plot
daily_pdf = (
    bronze
    .withColumn("calendar_date", F.to_date("timestamp"))
    .groupBy("calendar_date")
    .agg(
        F.avg("percent_observed").alias("avg_percent_observed"),
        F.min("percent_observed").alias("min_percent_observed"),
    )
    .orderBy("calendar_date")
    .toPandas()
)
print(daily_pdf)

fig, ax = plt.subplots(figsize=(11, 5))
ax.plot(daily_pdf["calendar_date"], daily_pdf["avg_percent_observed"], marker="o", markersize=3, color="#55A868")
ax.set_xlabel("date")
ax.set_ylabel("avg percent_observed")
ax.set_title("Bronze: daily average percent_observed (Sep-Oct 2025)")
ax.set_ylim(0, 100)
fig.autofmt_xdate()
fig.savefig(PLOTS_DIR / "percent_observed_daily_trend.png", dpi=150)
plt.show()
 