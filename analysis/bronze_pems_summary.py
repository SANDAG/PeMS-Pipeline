# %% [markdown]
# ## PeMS Bronze Layer: Data Quality Storyline
#
# Goal: understand bronze data quality well enough to build a pipeline that
# only carries forward good-quality data. Structure:
# 1. General summary of the raw bronze layer
# 2. The row-level filters silver already applies (`samples > 0`,
#    `total_flow IS NOT NULL` -- note this is NOT `total_flow > 0`), and how
#    much data/how many stations they drop
# 3. Deep dive on `percent_observed` (the detector-health signal), before
#    and after those filters, at both the raw-record and station-day grain
#
# Run cell by cell (VS Code: "Run Cell" above each `# %%`).
# Read-only against bronze_raw_pems_vsc; does not write anything back to
# Databricks and is not part of the DLT pipeline.

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

# %% [markdown]
# ### 1. General summary

# %% Distributions of key raw fields
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

# %% [markdown]
# ### 2. Filters the pipeline already applies (silver layer)
#
# `02-silver_pems_weekday_counts.py` drops bronze rows where `samples <= 0`
# OR `total_flow IS NULL` -- BEFORE it ever aggregates to station-day. Note
# this is `total_flow IS NOT NULL`, not `total_flow > 0`. `filtered_bronze`
# (rows passing both) is defined here and reused everywhere below to match
# exactly what silver actually sees.

# %% samples (0 vs >0) x total_flow (NULL vs NOT NULL) cross-tab
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

# %% Build filtered_bronze (silver's row-level filters) + stations dropped entirely
filtered_bronze = bronze.filter((F.col("samples") > 0) & F.col("total_flow").isNotNull())

total_stations = bronze.select("station").distinct().count()
stations_with_surviving_rows = filtered_bronze.select("station").distinct().count()
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

# %% [markdown]
# ### 3. Exploring percent_observed (the detector-health signal)
#
# Record-level buckets before/after the filters above, then station-day
# grain (interval availability + average percent_observed), matching
# silver's exact population (service_date, nonholiday Sep-Oct 2025
# weekdays) so the "100" buckets line up with what silver actually keeps.

# %% percent_observed bucket distribution (all bronze records) + plot
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

# %% percent_observed bucket distribution, AFTER silver's row-level filters + plot
# Same buckets as above, but restricted to filtered_bronze -- rows that
# structurally survive to be aggregated, before the 288-interval
# completeness gate is even considered. Answers: among the data silver
# doesn't immediately throw out, how much residual partial-imputation
# is left (vs. the dead-sensor rows already excluded)?
filtered_bucket_pdf = (
    filtered_bronze.withColumn("po_bucket", bucket_expr)
    .groupBy("po_bucket")
    .agg(F.count("*").alias("records"))
    .withColumn("pct_of_total", F.round(F.col("records") * 100.0 / F.sum("records").over(Window.partitionBy()), 2))
    .toPandas()
    .set_index("po_bucket")
    .reindex(PO_BUCKET_ORDER)
    .fillna(0)
    .reset_index()
)
print(filtered_bucket_pdf)

fig, ax = plt.subplots(figsize=(7, 5))
bars = ax.bar(filtered_bucket_pdf["po_bucket"], filtered_bucket_pdf["pct_of_total"], color="#4C72B0")
ax.set_xlabel("percent_observed bucket")
ax.set_ylabel("% of records passing samples>0 & total_flow filters")
ax.set_title("Bronze (post silver row-filters): percent_observed bucket distribution")
ax.set_ylim(0, filtered_bucket_pdf["pct_of_total"].max() * 1.25)
labels = [
    f"{int(records):,}\n({pct:.1f}%)"
    for records, pct in zip(filtered_bucket_pdf["records"], filtered_bucket_pdf["pct_of_total"])
]
ax.bar_label(bars, labels=labels, padding=3)
fig.savefig(PLOTS_DIR / "percent_observed_buckets_post_filter.png", dpi=150)
plt.show()

# %% Interval availability: % of 288 daily 5-min intervals present, per station-day
# Different from percent_observed above -- this measures whether a bronze row
# exists at all for each 5-min slot, not sensor health within a reported row.
#
# Matches silver_pems_weekday_counts_vsc's population and completeness gate
# exactly: starts from filtered_bronze (silver's samples>0 & total_flow
# IS NOT NULL row-level filters, applied BEFORE periods_day is computed --
# same order silver uses), then service_date (3AM-3AM, not calendar
# midnight), nonholiday Sep/Oct 2025 weekdays only. So the "100" bucket here
# = station-days actually kept in silver; everything else = filtered out.
holiday_dates = (
    spark.read.table("travel_data.pems.ref_pems_holidays_vsc")
    .select("holiday_date")
    .dropDuplicates(["holiday_date"])
)

service_dated = (
    filtered_bronze
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
total_stations_pop = interval_counts.select("station").distinct().count()
stations_with_clean_day = (
    interval_counts.filter(F.col("avg_percent_observed") == 100)
    .select("station")
    .distinct()
    .count()
)
stations_with_zero_clean_days = total_stations_pop - stations_with_clean_day

print(f"Total stations in population: {total_stations_pop:,}")
print(
    f"Stations with >=1 fully-observed (100%) station-day: "
    f"{stations_with_clean_day:,} ({stations_with_clean_day / total_stations_pop * 100:.1f}%)"
)
print(
    f"Stations with ZERO fully-observed station-days (fully dropped): "
    f"{stations_with_zero_clean_days:,} ({stations_with_zero_clean_days / total_stations_pop * 100:.1f}%)"
)

# %% Daily average percent_observed trend + plot
# Uses filtered_bronze (samples>0 & total_flow IS NOT NULL) -- same rows
# silver actually aggregates, so the dead-sensor rows already excluded from
# silver don't drag this trend down.
daily_pdf = (
    filtered_bronze
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
ax.set_title("Bronze (post silver row-filters): daily average percent_observed (Sep-Oct 2025)")
ax.set_ylim(0, 100)
fig.autofmt_xdate()
fig.savefig(PLOTS_DIR / "percent_observed_daily_trend.png", dpi=150)
plt.show()

# %% [markdown]
# ### What this means for a "good-quality data" pipeline
#
# - Silver's existing `samples > 0` / `total_flow IS NOT NULL` filters
#   already drop ~40% of raw bronze rows, and ~35% of stations lose *all*
#   their rows outright -- before the 288-interval completeness gate even
#   runs.
# - Among rows that DO survive those filters, station-days split roughly
#   evenly between fully clean (100% observed) and completely dead (0%
#   observed) -- there's very little "mostly good, a bit noisy."
# - Silver's `periods_day = 288` gate only checks that a row exists per
#   5-min slot, not whether `percent_observed` was healthy that day. A
#   dead-but-present sensor can still pass that gate if enough of its rows
#   individually clear `samples > 0` (partial-lane cases).
# - A genuine "good-quality data" guarantee would need a station-day-level
#   quality gate on `percent_observed` (e.g. `avg_percent_observed == 100`,
#   or some threshold) in addition to -- not instead of -- the existing
#   completeness check, since completeness and sensor health are
#   independent conditions.
