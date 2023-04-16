from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse
from prefect.filesystems import GCS


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcs-secret")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="sacred-alloy-375819",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow
def etl_gcs_to_bq_external_table():
    """Main ETL flow to load data into Big Query"""
    gcs_block = GCS.load("zoom-gcs")
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
