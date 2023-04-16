from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from typing import List
from prefect.filesystems import GCS
import io
import numpy as np
import kaggle


@task(retries=3, log_prints=True)
def fetch_spotify_dataset() -> Path:
    """Read taxi data from web into csv file"""
    dataset = "dhruvildave/spotify-charts"
    """
    kaggle.api.dataset_download_files(
        dataset=dataset,
        path="./data",
        quiet=False,
        force=True,
    )
    """
    return Path("./data/spotify-charts.zip")


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    Path(f"data/{color}").mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression="gzip")
    return path


@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local csv file to GCS"""
    gcs_block = GCS.load("zoom-gcs")
    with open(path, "r") as file:
        csv_data = file.read()
    gcs_block.write_path(path=path.as_posix(), content=csv_data.encode('utf-8'))


@task(log_prints=True)
def repartition_large_file(source_path: Path, chunksize: int = 1000000) -> List[Path]:
    path_list = []
    Path(f"./data/chunks").mkdir(parents=True, exist_ok=True)
    for i, chunk in enumerate(pd.read_csv(source_path, chunksize=chunksize)):
        chunk_file_path = Path(f"./data/chunks/charts_{i}.csv")
        chunk.to_csv(chunk_file_path)
        path_list.append(chunk_file_path)
        print(f"chunk {chunk_file_path} is created")
        break
    return path_list


@task(log_prints=True)
def clean(path: Path) -> None:
    """Fix dtype issues"""
    df = pd.read_csv(path)
    df["streams"] = df["streams"].astype('Int64')
    df["date"] = pd.to_datetime(df["date"])
    df.to_csv(path)
    print(f"chunk {path.as_posix()} is cleaned")


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    path = fetch_spotify_dataset()
    chunk_path_list = repartition_large_file(path)
    for chunk_path in chunk_path_list:
        clean(chunk_path)
        write_gcs(chunk_path)


if __name__ == "__main__":
    etl_web_to_gcs()
