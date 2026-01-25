#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

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

def run():
    pg_user = 'root'
    pg_password = 'root'
    pg_host = 'localhost'
    pg_db = 'ny_taxi'
    pg_port = 5432

    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

    year = 2021
    month = 1

    prefix_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
    url = prefix_url + f"yellow_tripdata_{year}-{month:02d}.csv.gz"

    df = pd.read_csv(url, dtype=dtype, parse_dates=parse_dates, nrows=100)

    chunksize = 100000
    target_table = "yellow_taxi_data"

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