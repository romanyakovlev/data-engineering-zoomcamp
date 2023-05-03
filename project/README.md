# Spotify Top Charts Project

## Project Goal

This project is developed to analyze the data from "TOP 200" charts based on [kaggle dataset](https://www.kaggle.com/datasets/dhruvildave/spotify-charts)

The goal is creating 4 charts:
1. Show the most popular song in Global region (the most frequent top 1 song)
2. Show the most popular artist in Global region (the sum of the most frequent top 1 songs)
3. Show the most popular song in all regions (the most frequent top 1 song)
4. Show the most popular artist in all regions (the sum of the most frequent top 1 songs)

## The result

[Looker Studio Report](https://lookerstudio.google.com/reporting/41370c48-d670-4648-ad94-eef6cb7a861c)

![page 1-2](https://github.com/romanyakovlev/data-engineering-zoomcamp/blob/main/project/imgs/1.jpg?raw=true)
![page 3-4](https://github.com/romanyakovlev/data-engineering-zoomcamp/blob/main/project/imgs/2.jpg?raw=true)

# Project Structure

The result is serverless application which has the following scheme

![flowchart](https://github.com/romanyakovlev/data-engineering-zoomcamp/blob/main/project/imgs/flowchart.png?raw=true)

## What is used

* Google Cloud Platform (GCP):        
  * Google Cloud Storage (GCS): Data Lake - stores raw data
  * BigQuery: Data Warehouse - stores optimized tables
  * Cloud Compute - runs prefect agent
  * Cloud Run - runs flow in a dockerized environment
  * DataProc - runs PySpark jobs in cloud cluster
* Terraform: Infrastructure-as-Code (IaC)
* Prefect: Workflow Orchestration
* Spark: Distributed Processing
* Looker studio: Data Visualisation

# How to run

## Prerequisites

To run project you need to:

1. Install the following requirements:
* [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)
* [Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)

2. Create an account in [Google Cloud Platform (GCP)](https://cloud.google.com/) 
3. Create project in GCP
4. Create service acccount with following roles:
* Artifact Registry Reader
* Artifact Registry Writer
* BigQuery Admin
* Cloud Run Admin
* Dataproc Administrator
* Dataproc Editor
* Editor
* Storage Admin
* Storage Object Admin 

5. Enable `Cloud Dataproc API`, `Compute Engine API `, `Cloud Run Admin API `, `Artifact Registry API ` and `BigQuery API` in GCP.

3. Generate service account credentials in json format.

4. Create .env file in root directory:
```sh
"     
GOOGLE_APPLICATION_CREDENTIALS=

PROJECT_ID=

KAGGLE_USERNAME=
KAGGLE_KEY=

PREFECT_CLOUD_API_KEY=

CLUSTER_NAME=
CLUSTER_REGION=
" > .env

```

5. Specify `GOOGLE_APPLICATION_CREDENTIALS` in `.env` as variable path to service account credentials.
6. Specify `PROJECT_ID` in `.env` as project id in GCP.
7. Register in Kaggle and create [API key](https://github.com/Kaggle/kaggle-api)
8. Specify `KAGGLE_KEY` in `.env` as API key and `KAGGLE_USERNAME` as username from Kaggle.
9. Register in [Prefect Cloud](https://app.prefect.cloud/), create API key and workspace.
10. Specify `PREFECT_CLOUD_API_KEY` in `.env` as API key from Prefect Cloud.
11. In Prefect create GCS `spotify-gcs` and GCP Credentials `gcp-creds` blocks.

## 1. Initialize Infrastructure

1. Run Infrastructure Initialization via Terraform (`1_init_infra.sh` script):
```sh
export $(cat .env | xargs)

terraform init
terraform plan -var="project=$PROJECT_ID"
terraform apply -var="project=$PROJECT_ID"
```
2. Specify `CLUSTER_NAME` as Dataproc cluster name
3. Specify `CLUSTER_REGION` as Dataproc cluster region

## 2. Prepare Environment

1. Prepare environment for deploy (`2_prepare_env.sh` script):

```sh
export $(cat .env | xargs)

# install dependencies
sudo apt-get install python3-venv
python3 -m venv spotify_project_venv
source spotify_project_venv/bin/activate
pip install -r requirements.txt

# start prefect agent
prefect cloud login -k $PREFECT_CLOUD_API_KEY
prefect agent start -q 'default'
```

## 3. Deploy

1. Run prefect agent

```sh

prefect agent start -q 'default'
```

2. Run flow in Prefect (`3_deploy.sh` script):

```sh
export $(cat .env | xargs)

source spotify_project_venv/bin/activate
prefect deployment build flows/etl_flow.py:etl_flow -n etl-flow --apply
prefect deployment apply etl_flow-deployment.yaml
prefect deployment run "Main flow/etl-flow"

```

