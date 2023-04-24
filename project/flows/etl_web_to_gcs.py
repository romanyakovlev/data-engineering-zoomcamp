import kaggle
import pandas as pd
from pathlib import Path
from typing import List
from prefect import flow, task
from prefect.filesystems import GCS


gcs_block = GCS.load("spotify-gcs")


@task(log_prints=True)
def fetch_spotify_dataset(dataset: str, dir_path: str, dataset_path: str) -> Path:
    """Fetch spotify dataset and return Path object"""
    kaggle.api.dataset_download_files(
        dataset=dataset,
        path=dir_path,
        quiet=False,
        force=True,
    )
    return Path(dataset_path)


@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local csv chunk file to GCS"""
    with open(path, "r") as file:
        csv_data = file.read()
    gcs_block.write_path(path=path.as_posix(), content=csv_data.encode('utf-8'))


@task(log_prints=True)
def repartition_large_file(source_path: Path, chunksize: int = 1000000) -> List[Path]:
    path_list = []
    Path(f"./data/chunks").mkdir(parents=True, exist_ok=True)
    c = 0
    for i, chunk in enumerate(pd.read_csv(source_path, chunksize=chunksize)):
        chunk_file_path = Path(f"./data/chunks/charts_{i}.csv")
        chunk.to_csv(chunk_file_path)
        path_list.append(chunk_file_path)
        print(f"chunk {chunk_file_path} is created")
        c += 1
        if c == 5:
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


@flow(log_prints=True, name="Subflow - Web to GCS step")
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    dataset, dir_path, dataset_path = "dhruvildave/spotify-charts", "./data", "./data/spotify-charts.zip"
    path = fetch_spotify_dataset(dataset, dir_path, dataset_path)
    chunk_path_list = repartition_large_file(path)
    for chunk_path in chunk_path_list:
        clean(chunk_path)
        write_gcs(chunk_path)


if __name__ == "__main__":
    etl_web_to_gcs()
