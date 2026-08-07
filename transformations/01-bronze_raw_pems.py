from pyspark import pipelines as dp 

dp.create_streaming_table(
    name="bronze_raw_pems_vsc",
    comment="Bronze ingestion of raw PeMS traffic data from Parquet files",
    table_properties={
        "delta.feature.timestampNtz": "supported"
    },
) 

@dp.append_flow(target="bronze_raw_pems_vsc", name="sept_2025")
def load_sept_2025():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load("/Volumes/travel_data/pems/raw_pems/station_5min/2025/09")
    )


@dp.append_flow(target="bronze_raw_pems_vsc", name="oct_2025")
def load_oct_2025():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load("/Volumes/travel_data/pems/raw_pems/station_5min/2025/10")
    )
