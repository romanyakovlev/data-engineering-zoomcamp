from prefect import task, flow
import re

from google.cloud import dataproc_v1
from google.cloud import storage
from prefect_gcp import GcpCredentials
from prefect.filesystems import GCS
from pathlib import Path


@task
def submit_dataproc_job(region, cluster_name, gcs_bucket, spark_filename, project_id):
    # Create the job client.
    gcp_credentials_block = GcpCredentials.load("gcp-creds")
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


@flow
def gcp_flow(*your_args):
    submit_dataproc_job(*your_args)


@flow
def upload_pyspark_jobs_gcs(local_path: str = "pyspark_jobs", to_path: str = "jobs"):
    gcs_block = GCS.load("spotify-gcs")
    gcs_block.put_directory(local_path=local_path, to_path=to_path)


if __name__ == "__main__":
    region = "europe-west6"
    cluster_name = "cluster-47d0"
    gcs_bucket = "spotify_data_lake_sacred-alloy-375819"
    spark_filename = "jobs/pyspark_job.py"
    project_id = "sacred-alloy-375819"
    upload_pyspark_jobs_gcs()
    gcp_flow(region, cluster_name, gcs_bucket, spark_filename, project_id)
