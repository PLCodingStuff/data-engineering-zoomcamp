# data-engineering-zoomcamp

# **IMPORTANT - NOT DONE YET** 

## Overview
This repo contains the work done for the first module of the [Data Engineering Zoomcamp 2026 from DataTalks.Club](https://datatalks.club/docs/courses/data-engineering-zoomcamp/). In this module we used Pyhton, Docker and PostgreSQL to create a data migration pipeline, from CSV a file with [2021 NY Taxi Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz) to a PostgreSQL Database. The CSV files are extracted and stored in the Database in chunks, using `pandas` framework, through a containerized CLI application. The PostreSQL Database runs in a multi-container application with PgAdmin and the Python script is executed in a separate container. The multi-container application must be up before using the script container.

## Requirements
This work is based on Docker for containerization. You must have Docker installed and running, in order to build and run the containers.

## How To Use

1. Clone this repo
   ```bash
   git clone https://github.com/PLCodingStuff/data-engineering-zoomcamp.git
   cd data-engineering-zoomcamp.git
   ```
2. Get into `pipeline` folder
   ```bash
   cd pipeline
   ```
2. Start the multi-container application
   ```bash
   docker compose up
   ```
3. Build the containerized script
   ```bash
   docker build -t taxi:v001 .
   ```
4. Check the network
   ```bash
   docker network ls
   ```
4. Run the container
   ```bash
   # The network name will be based on the directory or found with previous command
   docker run -it --rm \
   --network=pipeline_default \
   taxi:v001 \
   --pg-user=root \
   --pg-password=root \
   --pg-host=pgdatabase \
   --pg-port=5432 \
   --pg-db=ny_taxi \
   --target-table=yellow_taxi_trips
   ```
5. You can connect to PgAdmin through `localhost:8085`, with email `admin@admin.com` and password `root`.

## Configuration

You can modify the environment variables of PostgreSQL and PgAdmin in `docker-compose.yaml`. The default user and password in PostgreSQL are `root`, under the fields `POSTGRES_USER` and `POSTGRES_PASSWORD`. The default email in PgAdmin is `admin@admin.com` and the password is `root`, under the fields `PGADMIN_DEFAULT_EMAIL` and `PGADMIN_DEFAULT_PASSWORD`. If you are more familiar with docker configuration files, feel free to make any changes. Every change in `docker-compose.yaml` also entails a change to the container execution.
