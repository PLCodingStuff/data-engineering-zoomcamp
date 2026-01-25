#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]
import click

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-password', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2021, help='Year of the data')
@click.option('--month', default=1, help='Month of the data')
@click.option('--chunksize', default=100000, help='Chunk size for ingestion')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
def run(pg_user, pg_password, pg_host, pg_port, pg_db, year, month, chunksize, target_table):
    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

    prefix_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
    url = prefix_url + f"yellow_tripdata_{year}-{month:02d}.csv.gz"

    iter_df = pd.read_csv(url,
            dtype=dtype,
            parse_dates=parse_dates,
            iterator=True,
            chunksize=100000)

    first_chunk = next(iter_df)

    first_chunk.head(0).to_sql(name=target_table, 
                               con=engine, 
                               if_exists="replace")

    for df_chunk in tqdm(iter_df):
        df_chunk.to_sql(name=target_table, con=engine, if_exists='append')

if __name__ == "__main__":
    run()