import duckdb
from pathlib import Path

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
DB_PATH = "/data/taxi_rides_ny.duckdb"

def load_taxi_data(taxi_type: str):
    print(f"Loading {taxi_type} taxi data...")

    con = duckdb.connect(DB_PATH)

    # Explicit, deterministic settings
    con.execute("SET memory_limit='2GB'")
    con.execute("SET preserve_insertion_order=false")

    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    for year in [2019, 2020]:
        for month in range(1, 13):
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            url = f"{BASE_URL}/{taxi_type}/{filename}"

            print(f"Ingesting {filename}")

            # Stream directly from URL — no local file
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS raw.{taxi_type}_tripdata AS
                SELECT *
                FROM read_csv_auto(
                    '{url}',
                    compression='gzip'
                )
                LIMIT 0
            """)

            con.execute(f"""
                INSERT INTO raw.{taxi_type}_tripdata
                SELECT *
                FROM read_csv_auto(
                    '{url}',
                    compression='gzip'
                )
            """)

    con.close()
    print(f"Completed {taxi_type} taxi data.")

if __name__ == "__main__":
    for taxi_type in ["yellow", "green"]:
        load_taxi_data(taxi_type)