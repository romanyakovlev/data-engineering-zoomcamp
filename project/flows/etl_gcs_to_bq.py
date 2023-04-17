from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse
from prefect.filesystems import GCS


@flow
def etl_gcs_to_bq_external_table():
    """Main ETL flow to load data into Big Query"""
    gcs_block = GCS.load("spotify-gcs")
    query = """
        CREATE OR REPLACE EXTERNAL TABLE spotify.spotify_external_data
        OPTIONS (format = 'CSV', uris = ['gs://{bucket_path}/data/chunks/charts_*.csv']);
        """.format(bucket_path=gcs_block.bucket_path)
    gcp_credentials = GcpCredentials.load("gcp-creds")
    client = gcp_credentials.get_bigquery_client()
    client.create_dataset("spotify", exists_ok=True)
    with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
        warehouse.execute(query)


@flow
def etl_bq_external_table_to_optimized_table():
    query = """
        CREATE OR REPLACE TABLE spotify.spotify_data
        PARTITION BY DATE(date)
        CLUSTER BY region AS
        SELECT title, rank, CAST(date AS TIMESTAMP) as date, artist, url, region, chart, trend, CAST(streams as INT64) as streams 
        FROM spotify.spotify_external_data;
        """
    gcp_credentials = GcpCredentials.load("gcp-creds")
    client = gcp_credentials.get_bigquery_client()
    client.create_dataset("spotify", exists_ok=True)
    with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
        warehouse.execute(query)


@flow(log_prints=True)
def etl_parent_flow():
    """Main ETL flow to load data into Big Query"""
    etl_gcs_to_bq_external_table()
    etl_bq_external_table_to_optimized_table()


if __name__ == "__main__":
    etl_parent_flow()
