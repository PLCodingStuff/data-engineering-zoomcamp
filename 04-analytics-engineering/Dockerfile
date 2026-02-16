FROM python:3.13-slim

WORKDIR /app


RUN apt-get update && apt-get install -y gcc libffi-dev && rm -rf /var/lib/apt/lists/*

RUN pip install duckdb dbt-duckdb

RUN mkdir -p data/
VOLUME ["/data"]

# RUN mkdir -p /app/dbt_project/models /app/dbt_project/analysis /app/dbt_project/tests /app/dbt_project/macros /app/dbt_project/data

# RUN echo "name: taxi_rides_ny" > /app/dbt_project/dbt_project.yml
# RUN echo "version: '1.0'" > /app/dbt_project/dbt_project.yml
# RUN echo "config-version: 2" > /app/dbt_project/dbt_project.yml
# RUN echo "profile: taxi_rides_ny" > /app/dbt_project/dbt_project.yml
# RUN echo "target-path: target" > /app/dbt_project/dbt_project.yml
# RUN echo "model-paths: ['models']" > /app/dbt_project/dbt_project.yml
# RUN echo "analysis-paths: ['analysis']" > /app/dbt_project/dbt_project.yml
# RUN echo "test-paths: ['tests'] " > /app/dbt_project/dbt_project.yml
# RUN echo "data-paths: ['data'] " > /app/dbt_project/dbt_project.yml
# RUN echo "macro-paths: ['macros'] " > /app/dbt_project/dbt_project.yml

RUN mkdir -p /root/.dbt
COPY profiles.yml /root/.dbt/profiles.yml

COPY load_ny_taxi_data.py .

CMD ["bash"]