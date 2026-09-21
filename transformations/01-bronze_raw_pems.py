from pyspark import pipelines as dp 

dp.create_streaming_table(
    name="bronze_raw_pems",
    comment="Bronze ingestion of raw PeMS traffic data from Parquet files",
    table_properties={
        "delta.feature.timestampNtz": "supported"
    },
) 


def _register_load_flow(year: str, month_num: str, month_name: str):
    @dp.append_flow(target="bronze_raw_pems", name=f"{month_name}_{year}")
    def _load():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .load(f"/Volumes/travel_data/pems/raw_pems/station_5min/{year}/{month_num}")
        )

    return _load


for _year in ("2022", "2023", "2024", "2025"):
    for _month_num, _month_name in (("09", "sept"), ("10", "oct")):
        globals()[f"load_{_month_name}_{_year}"] = _register_load_flow(
            _year, _month_num, _month_name
        )
