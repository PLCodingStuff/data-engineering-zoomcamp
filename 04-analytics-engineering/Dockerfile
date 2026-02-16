FROM python:3.13-slim

WORKDIR /app


RUN apt-get update && apt-get install -y gcc libffi-dev && rm -rf /var/lib/apt/lists/*

RUN pip install duckdb dbt-duckdb

RUN mkdir -p data/
VOLUME ["/data"]

RUN mkdir -p /root/.dbt
COPY profiles.yml /root/.dbt/profiles.yml

COPY load_ny_taxi_data.py .

CMD ["bash"]