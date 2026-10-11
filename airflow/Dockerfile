# Airflow 2.11 (last 2.x line), Python 3.12 to match the project.
FROM apache/airflow:2.11.2-python3.12

# The job's Postgres layer uses psycopg 3; the base image ships only psycopg2.
RUN pip install --no-cache-dir "psycopg[binary]>=3.1,<4"
