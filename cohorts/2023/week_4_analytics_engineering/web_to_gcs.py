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
def write_local(df: pd.DataFrame, dataset_file: str, data_name) -> Path:
    """Write DataFrame out locally as csv file"""
    path = Path(f"data/{data_name}/{dataset_file}.csv.gz")
    Path(f"data/{data_name}").mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression="gzip")
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
    years = [2019, 2020]
    for data_name in ["green", "yellow"]:
        for year in years:
            for month in range(1, 13):
                dataset_file = f"{data_name}_tripdata_{year}-{month:02}"
                dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{data_name}/{dataset_file}.csv.gz"
                df = fetch(dataset_url)
                path = write_local(df, dataset_file, data_name)
                write_gcs(path)
    data_name = "fhv"
    year = 2019
    for month in range(1, 13):
        dataset_file = f"{data_name}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{data_name}/{dataset_file}.csv.gz"
        df = fetch(dataset_url)
        path = write_local(df, dataset_file, data_name)
        write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()
