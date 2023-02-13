from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3, log_prints=True)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    print(dataset_url)
    df = pd.read_csv(dataset_url)
    return df


@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as csv file"""
    path = Path(f"data/fhv/{dataset_file}.csv.gz")
    Path(f"data/fhv").mkdir(parents=True, exist_ok=True)
    df.to_csv(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    year = 2019
    for month in range(1, 13):
        dataset_file = f"fhv_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
        df = fetch(dataset_url)
        path = write_local(df, dataset_file)
        write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()
