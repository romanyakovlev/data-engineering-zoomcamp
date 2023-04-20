import re

from prefect import task, flow
from prefect.filesystems import GCS
from prefect_gcp import GcpCredentials
from google.cloud import dataproc_v1, storage


gcs_block = GCS.load("spotify-gcs")
gcp_credentials_block = GcpCredentials.load("gcp-creds")


@task
def submit_dataproc_job(region: str, cluster_name: str, gcs_bucket: str, spark_filename:str, project_id: str) -> None:
    # Create the job client.
    job_client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)},
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
    )
    # Create the job config.
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": "gs://{}/{}".format(gcs_bucket, spark_filename),
            "jar_file_uris": ("gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",),
        },
    }
    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    response = operation.result()
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)
    output = (
        storage.Client(credentials=gcp_credentials_block.get_credentials_from_service_account())
        .get_bucket(matches.group(1))
        .blob(f"{matches.group(2)}.000000000")
        .download_as_string()
    )
    print(f"Job finished successfully: {output}\r\n")


@task
def upload_pyspark_jobs_to_gcs(local_path: str = "pyspark_jobs", to_path: str = "jobs") -> None:
    gcs_block.put_directory(local_path=local_path, to_path=to_path)


@flow
def etl_transform_data(
        region: str,
        cluster_name: str,
        gcs_bucket: str,
        spark_filename:str,
        project_id: str,
        pyspark_local_path: str,
        pyspark_gcs_path: str
):
    upload_pyspark_jobs_to_gcs(pyspark_local_path, pyspark_gcs_path)
    submit_dataproc_job(region, cluster_name, gcs_bucket, spark_filename, project_id)


if __name__ == "__main__":
    region = "us-central1"
    cluster_name = "spotify-cluster-sacred-alloy-375819"
    gcs_bucket = "spotify_data_lake_sacred-alloy-375819"
    spark_filename = "jobs/pyspark_job.py"
    project_id = "sacred-alloy-375819"
    pyspark_local_path = "pyspark_jobs"
    pyspark_gcs_path = "jobs"
    etl_transform_data(
        region,
        cluster_name,
        gcs_bucket,
        spark_filename,
        project_id,
        pyspark_local_path,
        pyspark_gcs_path
    )
