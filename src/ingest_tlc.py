"""Land the official TLC January 2025 Yellow Taxi file unchanged."""
from pathlib import Path
from ingest_http import ingest, ROOT

SOURCE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"

if __name__ == "__main__":
    ingest(SOURCE, ROOT / "data/raw", "nyc_tlc_yellow", "yellow_tripdata_2025-01.parquet")
